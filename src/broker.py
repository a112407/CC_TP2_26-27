import socket, sys, threading, time
import wire
from topics import candidate_filters, matches, valid_filter, valid_topic


class Broker:
    def __init__(self, cfg, ident):
        self.ident = ident
        self.bind = cfg.get("bind")
        self.control_port = cfg.get_int("control_port")
        self.data_port = cfg.get_int("data_port")
        self.duration = cfg.get_float("duration")
        self.evict_after = cfg.get_float("evict_after")
        self.lock = threading.Lock()
        # Tabela 1 do desenho: filtro -> {session_id: qos}  ("Tópico | Inscritos")
        self.table = {}
        # Tabela 2 do desenho: informação do tópico + subscritores ATIVOS.
        # topico -> {"qos": qos do fluxo (REG), "pub": id do publicador,
        #            "subs": {session_id: qos mais forte dos filtros que o cobrem}}
        # É a tabela 1 já resolvida para tópicos concretos: na publicação basta 1 consulta.
        self.topics = {}
        # Estado por sessão (um dicionário por papel):
        #   {addr: (IP do TCP, porta UDP do HELLO), tx: último tx_seq, id, conn}
        self.subs = {}      # session_id -> subscritor
        self.pubs = {}      # session_id -> publicador
        # Associa a origem de um datagrama UDP à sessão que a anunciou no HELLO.
        # Serve para aceitar DATA só de publicadores com sessão e, na fase 2,
        # para ligar cada ACK UDP ao emissor.
        self.by_addr = {}   # (ip, porta UDP) -> session_id
        self.next_session = 1
        self.stop = threading.Event()
        self.retransmit = {}  # dest -> {tx_seq: {"pkt": pacote, "last": ts}}
        self.dup_seen = {}     # (sid_pub, tx_seq) -> ts: deduplicação de DATA retransmitidos

    def warn(self, msg):
        print("[broker %s] %s" % (self.ident, msg), file=sys.stderr, flush=True)

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
        threading.Thread(target=self.retransmit_loop, daemon=True).start()
        threading.Thread(target=self.evict_loop, daemon=True).start()
        deadline = time.time() + self.duration
        try:
            while time.time() < deadline:
                try:
                    data, addr = self.udp.recvfrom(65535)
                except socket.timeout:
                    continue
                except OSError:
                    time.sleep(0.01)
                    continue
                try:
                    ack = wire.dec_udp_ack(data)
                    if ack is not None:
                        self.on_udp_ack(addr, ack)
                        continue
                    self.on_data(data, addr)
                except Exception as e:
                    self.warn("on_data: %r" % (e,))
        finally:
            self.stop.set()
            for sock in (getattr(self, "tcp", None), getattr(self, "udp", None)):
                if sock is not None:
                    try:
                        sock.shutdown(socket.SHUT_RDWR)
                    except OSError:
                        pass
                    try:
                        sock.close()
                    except OSError:
                        pass
            for st in list(self.subs.values()):
                try:
                    st["conn"].shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                try:
                    st["conn"].close()
                except OSError:
                    pass

    # ---------- plano de controlo ----------
    def accept_loop(self):
        while not self.stop.is_set():
            try:
                conn, addr = self.tcp.accept()
            except socket.timeout:
                continue
            except OSError:
                time.sleep(0.05)
                continue
            threading.Thread(target=self.session, args=(conn, addr), daemon=True).start()

    def session(self, conn, addr):
        conn.settimeout(None)
        rdr = wire.FrameReader(conn)
        sid = None
        udp_addr = None
        try:
            first = rdr.read()
            if first is None or first[0] != wire.T_HELLO:
                return
            role, udp_port, cid = wire.dec_hello(first[2])
            # ROLE_BROKER só na fase 4; porta UDP 0 não é endereçável.
            if role not in (wire.ROLE_PUB, wire.ROLE_SUB) or udp_port == 0:
                return
            udp_addr = (addr[0], udp_port)
            with self.lock:
                sid = self.next_session
                self.next_session += 1
                st = {"addr": udp_addr, "tx": 0, "id": cid, "conn": conn, "last_seen": time.time()}
                (self.subs if role == wire.ROLE_SUB else self.pubs)[sid] = st
                self.by_addr[udp_addr] = sid
            conn.sendall(wire.enc_welcome(self.data_port, sid))
            while True:
                m = rdr.read()
                if m is None or m[0] == wire.T_BYE:
                    break
                mtype, qos, payload = m
                text = payload.decode(errors="replace")
                # Regra do SubControl: TODO o comando recebe uma resposta.
                # Por omissão é recusado (tipo desconhecido, comando que não
                # pertence ao papel da sessão, argumentos inválidos).
                status = 1
                if mtype == wire.T_REG and role == wire.ROLE_PUB:
                    if valid_topic(text) and qos in (1, 2):
                        with self.lock:
                            self.topics[text] = {"qos": qos, "pub": cid,
                                                 "subs": self._resolve(text)}
                        status = 0
                elif mtype == wire.T_SUB and role == wire.ROLE_SUB:
                    if valid_filter(text) and qos in (1, 2):
                        with self.lock:
                            self.table.setdefault(text, {})[sid] = qos
                            self._refresh(text)
                        status = 0
                elif mtype == wire.T_UNSUB and role == wire.ROLE_SUB:
                    with self.lock:
                        self._drop(text, sid)
                        self._refresh(text)
                    status = 0
                conn.sendall(wire.enc_ack(status))
        except (OSError, IndexError, ValueError):
            pass
        finally:
            if sid is not None:
                with self.lock:
                    for f in list(self.table):
                        self._drop(f, sid)
                    self._refresh()          # deixa de ser subscritor ativo
                    self.subs.pop(sid, None)
                    self.pubs.pop(sid, None)
                    if udp_addr is not None:
                        self.retransmit.pop(udp_addr, None)
                    if self.by_addr.get(udp_addr) == sid:
                        del self.by_addr[udp_addr]
            try:
                conn.close()
            except OSError:
                pass

    def _drop(self, flt, sid):  # chamar com o lock
        inscritos = self.table.get(flt)
        if inscritos is not None:
            inscritos.pop(sid, None)
            if not inscritos:
                del self.table[flt]

    def _evict_subscriber(self, sid):  # chamar com o lock
        st = self.subs.get(sid)
        if st is None:
            return
        udp_addr = st["addr"]
        for f in list(self.table):
            self._drop(f, sid)
        self._refresh()
        self.subs.pop(sid, None)
        if self.by_addr.get(udp_addr) == sid:
            del self.by_addr[udp_addr]
        self.retransmit.pop(udp_addr, None)
        try:
            st["conn"].shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            st["conn"].close()
        except OSError:
            pass

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
        (ou de todos se flt for None). Substitui o dicionário inteiro: quem
        já copiou a referência nunca vê uma atualização a meio."""
        for t, info in self.topics.items():
            if flt is None or matches(flt, t):
                info["subs"] = self._resolve(t)

    def _is_duplicate_pub(self, sid_pub, tx_seq):
        now = time.time()
        window = 2 * wire.RTO
        for key, ts in list(self.dup_seen.items()):
            if now - ts > window:
                del self.dup_seen[key]
        key = (sid_pub, tx_seq)
        if key in self.dup_seen:
            return True
        self.dup_seen[key] = now
        return False

    # ---------- plano de dados ----------
    def retransmit_loop(self):
        while not self.stop.is_set():
            now = time.time()
            due = []
            with self.lock:
                for dest, pending in self.retransmit.items():
                    for tx_seq, state in list(pending.items()):
                        if now - state["last"] >= wire.RTO:
                            state["last"] = now
                            due.append((dest, state["pkt"]))
            for dest, pkt in due:
                try:
                    self.udp.sendto(pkt, dest)
                except OSError:
                    pass
            time.sleep(0.05)

    def evict_loop(self):
        while not self.stop.is_set():
            now = time.time()
            with self.lock:
                expired = [sid for sid, s in self.subs.items()
                           if s["addr"] in self.retransmit
                           and now - s["last_seen"] >= self.evict_after]
                for sid in expired:
                    self._evict_subscriber(sid)
            time.sleep(0.1)

    def on_udp_ack(self, addr, tx_seq):
        with self.lock:
            sid = self.by_addr.get(addr)
            if sid in self.subs:
                self.subs[sid]["last_seen"] = time.time()
            pending = self.retransmit.get(addr)
            if pending is None:
                return
            pending.pop(tx_seq, None)
            if not pending:
                del self.retransmit[addr]

    def on_data(self, data, addr):
        d = wire.dec_data(data)
        if d is None or not valid_topic(d[3]):
            return  # truncado / desconhecido / tópico inválido: descarta
        _, tx_seq, msg_id, topic, content = d
        with self.lock:
            sid_pub = self.by_addr.get(addr)
            if sid_pub not in self.pubs:
                return  # origem sem sessão de publicador: descarta
            duplicate = self._is_duplicate_pub(sid_pub, tx_seq)
            self.pubs[sid_pub]["last_seen"] = time.time()
            info = self.topics.get(topic)
            if info is None:
                return  # fluxo não registado (REG): descarta
            qos_flow = info["qos"]
            targets = []
            if not duplicate:
                for sid, q in info["subs"].items():     # 1 consulta, 1 cópia por subscritor
                    s = self.subs.get(sid)
                    if s is None:
                        continue
                    seq, s["tx"] = s["tx"], wire.seq_next(s["tx"])  # tx_seq por subscritor, com volta
                    pkt = wire.enc_data(min(q, qos_flow), seq, msg_id, topic, content)
                    queue = self.retransmit.setdefault(s["addr"], {})
                    if not queue:
                        s["last_seen"] = time.time()
                    queue[seq] = {"pkt": pkt, "last": time.time()}
                    targets.append((s["addr"], min(q, qos_flow), seq, pkt))
        try:
            self.udp.sendto(wire.enc_udp_ack(tx_seq), addr)
        except OSError:
            pass
        if duplicate:
            return
        for dest, qos, tx, pkt in targets:
            try:
                self.udp.sendto(pkt, dest)
            except OSError as e:
                # ex.: rede inalcançável. Um destino em falha não afeta os
                # outros; na fase 2 a retransmissão volta a tentar.
                self.warn("sendto %s: %r" % (dest, e))
