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

        # Aqui ficam as subscrições tal como foram recebidas:
        # filtro -> {id_da_sessao: qos}.
        self.table = {}

        # Nos tópicos guardamos também os subscritores já resolvidos.
        # Assim, quando chega uma mensagem, não temos de voltar a procurar
        # todos os filtros possíveis.
        self.topics = {}

        # Estado separado para publicadores e subscritores.
        self.subs = {}
        self.pubs = {}

        # O UDP não tem sessão. Este mapa diz-nos a que sessão pertence
        # cada endereço UDP anunciado no HELLO.
        self.by_addr = {}
        self.next_session = 1
        self.stop = threading.Event()

        # Para cada subscritor, guardamos os pacotes que ainda não tiveram ACK.
        self.retransmit = {}

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

    # Primeiro tratamos as ligações TCP e os comandos de controlo.
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
            if role not in (wire.ROLE_PUB, wire.ROLE_SUB) or udp_port == 0:
                return
            udp_addr = (addr[0], udp_port)
            with self.lock:
                sid = self.next_session
                self.next_session += 1
                st = {"addr": udp_addr, "tx": 0, "id": cid, "conn": conn, "last_seen": time.time()}
                (self.subs if role == wire.ROLE_SUB else self.pubs)[sid] = st
                self.by_addr[udp_addr] = sid

            # A partir daqui o cliente já sabe em que porta UDP deve falar
            # com o broker e nós conseguimos validar a origem dos datagramas.
            conn.sendall(wire.enc_welcome(self.data_port, sid))
            while True:
                m = rdr.read()
                if m is None or m[0] == wire.T_BYE:
                    break
                mtype, qos, payload = m
                text = payload.decode(errors="replace")

                # Começamos por recusar. Só damos OK quando o comando faz
                # sentido para o papel da sessão e os argumentos são válidos.
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
                        self._purge_pending(sid)
                    status = 0
                conn.sendall(wire.enc_ack(status))
        except (OSError, IndexError, ValueError):
            pass
        finally:
            if sid is not None:
                with self.lock:
                    # Fechar a ligação também cancela as subscrições e limpa os
                    # pacotes pendentes dessa sessão.
                    for f in list(self.table):
                        self._drop(f, sid)
                    self._refresh()
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

    def _drop(self, flt, sid):
        # Este método é chamado com self.lock já adquirido.
        inscritos = self.table.get(flt)
        if inscritos is not None:
            inscritos.pop(sid, None)
            if not inscritos:
                del self.table[flt]

    def _evict_subscriber(self, sid):
        # Se um subscritor deixa de responder, removêmo-lo para não ficarmos
        # a retransmitir para sempre.
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

    def _resolve(self, topic):
        # Um subscritor pode aparecer em mais do que um filtro. Nesse caso,
        # fica com o QoS mais forte entre os filtros que correspondem.
        best = {}
        for f in candidate_filters(topic):
            for sid, q in self.table.get(f, {}).items():
                best[sid] = max(best.get(sid, 0), q)
        return best

    def _refresh(self, flt=None):
        # Recalculamos só os tópicos afetados por uma alteração de filtro.
        for t, info in self.topics.items():
            if flt is None or matches(flt, t):
                info["subs"] = self._resolve(t)
    
    def _purge_pending(self, sid):
        # Chamado com self.lock. Descarta da fila do subscritor os pacotes
        # de tópicos que já não lhe correspondem (depois de um UNSUB).
        st = self.subs.get(sid)
        if st is None:
            return
        pending = self.retransmit.get(st["addr"])
        if not pending:
            return
        for seq, state in list(pending.items()):
            info = self.topics.get(state["topic"])
            if info is None or sid not in info["subs"]:
                del pending[seq]
        if not pending:
            del self.retransmit[st["addr"]]

    def _is_duplicate_pub(self, sid_pub, tx_seq):
        # Chamado com self.lock adquirido. O conjunto vive com a sessão do
        # publicador (self.pubs) e desaparece quando ela fecha.
        seen = self.pubs[sid_pub].setdefault("seen", set())
        if tx_seq in seen:
            return True
        seen.add(tx_seq)
        return False

    # Depois do controlo, estas rotinas tratam dos DATA, ACKs e retransmissões.
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
                    # O pacote continua na fila: a próxima volta tenta outra vez.
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
            return
        _, tx_seq, msg_id, topic, content = d
        with self.lock:
            sid_pub = self.by_addr.get(addr)
            if sid_pub not in self.pubs:
                return

            # O ACK do publicador é tratado mesmo quando o DATA é repetido.
            # O que não fazemos é encaminhar esse DATA outra vez.
            duplicate = self._is_duplicate_pub(sid_pub, tx_seq)
            self.pubs[sid_pub]["last_seen"] = time.time()
            info = self.topics.get(topic)
            targets = []
            if info is not None and not duplicate:
                qos_flow = info["qos"]
                for sid, q in info["subs"].items():
                    s = self.subs.get(sid)
                    if s is None:
                        continue
                    # Cada salto tem o seu próprio contador de sequência:
                    # o tx_seq do publicador não é reutilizado para o subscritor.
                    seq, s["tx"] = s["tx"], wire.seq_next(s["tx"])
                    pkt = wire.enc_data(min(q, qos_flow), seq, msg_id, topic, content)
                    queue = self.retransmit.setdefault(s["addr"], {})
                    if not queue:
                        s["last_seen"] = time.time()
                    queue[seq] = {"pkt": pkt, "last": time.time(), "topic": topic}
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
                # Não falhamos a entrega aos outros subscritores por causa
                # de um destino; a fila fica pronta para a retransmissão.
                self.warn("sendto %s: %r" % (dest, e))
