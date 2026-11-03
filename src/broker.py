import socket, threading, time
import wire
from topics import candidate_filters, matches, valid_filter, valid_topic


class Broker:
    def __init__(self, cfg, ident):
        self.ident = ident
        self.bind = cfg.get("bind")
        self.control_port = cfg.get_int("control_port")
        self.data_port = cfg.get_int("data_port")
        self.duration = cfg.get_float("duration")
        self.lock = threading.Lock()
        # Tabela 1 do desenho: filtro -> {session_id: qos}  ("Tópico | Inscritos")
        self.table = {}
        # Tabela 2 do desenho: informação do tópico + subscritores ATIVOS.
        # topico -> {"qos": qos do fluxo (REG), "pub": id do publicador,
        #            "subs": {session_id: qos mais forte dos filtros que o cobrem}}
        # É a tabela 1 já resolvida para tópicos concretos: na publicação basta 1 consulta.
        self.topics = {}
        self.subs = {}      # session_id -> {addr, tx, id}  (estado por subscritor)
        self.next_session = 1
        self.stop = threading.Event()

    def run(self):
        self.tcp = socket.socket()
        self.tcp.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.tcp.bind((self.bind, self.control_port))
        self.tcp.listen(32)
        self.tcp.settimeout(0.2)
        self.udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp.bind((self.bind, self.data_port))
        self.udp.settimeout(0.2)
        threading.Thread(target=self.accept_loop, daemon=True).start()
        deadline = time.time() + self.duration
        while time.time() < deadline:
            try:
                data, addr = self.udp.recvfrom(65535)
            except socket.timeout:
                continue
            self.on_data(data)
        self.stop.set()

    # ---------- plano de controlo ----------
    def accept_loop(self):
        while not self.stop.is_set():
            try:
                conn, addr = self.tcp.accept()
            except socket.timeout:
                continue
            threading.Thread(target=self.session, args=(conn, addr), daemon=True).start()

    def session(self, conn, addr):
        conn.settimeout(None)
        rdr = wire.FrameReader(conn)
        sid = None
        try:
            first = rdr.read()
            if first is None or first[0] != wire.T_HELLO:
                return
            role, udp_port, cid = wire.dec_hello(first[2])
            with self.lock:
                sid = self.next_session
                self.next_session += 1
                if role == wire.ROLE_SUB:
                    self.subs[sid] = {"addr": (addr[0], udp_port), "tx": 0, "id": cid}
            conn.sendall(wire.enc_welcome(self.data_port, sid))
            while True:
                m = rdr.read()
                if m is None or m[0] == wire.T_BYE:
                    break
                mtype, qos, payload = m
                text = payload.decode(errors="replace")
                if mtype == wire.T_REG:
                    ok = valid_topic(text) and qos in (1, 2)
                    if ok:
                        with self.lock:
                            self.topics[text] = {"qos": qos, "pub": cid,
                                                 "subs": self._resolve(text)}
                    conn.sendall(wire.enc_ack(0 if ok else 1))
                elif mtype == wire.T_SUB and sid in self.subs:
                    ok = valid_filter(text) and qos in (1, 2)
                    if ok:
                        with self.lock:
                            self.table.setdefault(text, {})[sid] = qos
                            self._refresh(text)
                    conn.sendall(wire.enc_ack(0 if ok else 1))
                elif mtype == wire.T_UNSUB and sid in self.subs:
                    with self.lock:
                        self._drop(text, sid)
                        self._refresh(text)
                    conn.sendall(wire.enc_ack(0))
        except (OSError, IndexError):
            pass
        finally:
            with self.lock:
                for f in list(self.table):
                    self._drop(f, sid)
                self._refresh()          # deixa de ser subscritor ativo
                self.subs.pop(sid, None)
            conn.close()

    def _drop(self, flt, sid):  # chamar com o lock
        inscritos = self.table.get(flt)
        if inscritos is not None:
            inscritos.pop(sid, None)
            if not inscritos:
                del self.table[flt]

    def _resolve(self, topic):  # chamar com o lock
        """Subscritores que cobrem um tópico concreto: 3 consultas à tabela 1;
        com filtros sobrepostos fica o qos mais forte."""
        best = {}
        for f in candidate_filters(topic):
            for sid, q in self.table.get(f, {}).items():
                best[sid] = max(best.get(sid, 0), q)
        return best

    def _refresh(self, flt=None):  # chamar com o lock
        """Recalcula os subscritores dos tópicos registados afetados por 'flt'
        (ou de todos se flt for None)."""
        for t, info in self.topics.items():
            if flt is None or matches(flt, t):
                info["subs"] = self._resolve(t)

    # ---------- plano de dados ----------
    def on_data(self, data):
        d = wire.dec_data(data)
        if d is None or not valid_topic(d[3]):
            return  # truncado / desconhecido: descarta
        qos_hdr, _, msg_id, topic, content = d
        with self.lock:
            info = self.topics.get(topic)
            if info is not None:              # fluxo registado: 1 consulta
                subs, qos_flow = info["subs"], info["qos"]
            else:                             # não registado: resolve na hora
                subs, qos_flow = self._resolve(topic), qos_hdr
            targets = []
            for sid, q in subs.items():
                s = self.subs[sid]
                s["tx"] += 1
                targets.append((s["addr"], min(q, qos_flow), s["tx"]))  # uma cópia
        for addr, qos, tx in targets:
            self.udp.sendto(wire.enc_data(qos, tx, msg_id, topic, content), addr)