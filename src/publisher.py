import socket, sys, threading, time
import wire
from logutil import DeliveryLog


ACK_TIMEOUT = wire.RTO


def run_publisher(cfg, ident):
    host, port = cfg.get_addr("broker_control")
    log = DeliveryLog(cfg.get("log"), ident)
    start_after = cfg.get_float("start_after")
    drain = cfg.get_float("drain")

    udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp.bind(("0.0.0.0", 0))
    tcp = socket.create_connection((host, port))
    tcp.sendall(wire.enc_hello(wire.ROLE_PUB, udp.getsockname()[1], ident))
    m = wire.FrameReader(tcp).read()
    if m is None or m[0] != wire.T_WELCOME:
        raise SystemExit("sessão recusada")
    data_port, _ = wire.dec_welcome(m[2])
    broker_data = (host, data_port)
    rdr = wire.FrameReader(tcp)
    # Antes de publicar, o broker tem de conhecer todos os fluxos deste
    # publicador.
    for topic, o in cfg.all("publish"):
        tcp.sendall(wire.enc_reg(topic, int(o["qos"])))
        r = rdr.read()
        if r is None or r[0] != wire.T_ACK or r[2][0] != 0:
            raise SystemExit("registo recusado: " + topic)

    pending = {}
    lock = threading.Lock()
    stop = threading.Event()

    def ack_loop():
        # Cada ACK tira um pacote da fila. Enquanto ele lá estiver,
        # a outra thread pode voltar a enviá-lo.
        udp.settimeout(0.2)
        while not stop.is_set():
            try:
                data, _ = udp.recvfrom(65535)
            except socket.timeout:
                continue
            ack = wire.dec_udp_ack(data)
            if ack is None:
                continue
            with lock:
                pending.pop(ack, None)

    def resend_loop():
        # Os pacotes sem ACK ficam pendentes até serem confirmados ou até
        # acabar o tempo de espera do publicador.
        while not stop.is_set():
            now = time.time()
            with lock:
                due = [(tx_seq, state) for tx_seq, state in pending.items()
                       if now - state["last"] >= ACK_TIMEOUT]
            for tx_seq, state in due:
                try:
                    udp.sendto(state["pkt"], broker_data)
                except OSError:
                    pass
                with lock:
                    if tx_seq in pending and pending[tx_seq] is state:
                        state["last"] = now
            time.sleep(0.05)

    threads = [
        threading.Thread(target=ack_loop, daemon=True),
        threading.Thread(target=resend_loop, daemon=True),
    ]
    for t in threads:
        t.start()

    # A agenda guarda quando cada mensagem deve sair, contado desde o arranque
    # da publicação.
    sched = []
    for topic, o in cfg.all("publish"):
        count, rate, qos = int(o["count"]), float(o["rate"]), int(o["qos"])
        sched += [(k / rate, topic, qos, k) for k in range(count)]
    sched.sort()

    tx = 0
    time.sleep(start_after)
    t0 = time.time()
    for rel, topic, qos, mid in sched:
        wait = t0 + rel - time.time()
        if wait > 0:
            time.sleep(wait)
        seq, tx = tx, wire.seq_next(tx)
        # Registamos antes do sendto para a publicação nunca aparecer depois
        # da entrega no log do subscritor (os relógios são guardados em ms).
        log.write(topic, mid)
        pkt = wire.enc_data(qos, seq, mid, topic, b"synthetic")
        with lock:
            pending[seq] = {"pkt": pkt, "last": time.time()}
        try:
            udp.sendto(pkt, broker_data)
        except OSError as e:
            # A mensagem já ficou registada e continua pendente, por isso
            # a thread de retransmissão ainda pode tentar enviá-la.
            print("[publisher %s] sendto: %r" % (ident, e), file=sys.stderr, flush=True)

    # Damos algum tempo para os ACKs chegarem. Se todos chegaram, podemos
    # terminar logo; caso contrário, esperamos no máximo o valor de drain.
    deadline = time.time() + drain
    while time.time() < deadline:
        with lock:
            if not pending:
                break
        time.sleep(0.02)
    stop.set()
    for t in threads:
        t.join(1)
    try:
        tcp.sendall(wire.enc_frame(wire.T_BYE))
    except OSError:
        pass
    tcp.close()
