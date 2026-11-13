import socket, sys, threading, time
import wire
from logutil import DeliveryLog


LINGER = 0.5   # s, espera antes do BYE (ver abaixo)
ACK_TIMEOUT = 0.25


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
    for topic, o in cfg.all("publish"):      # registo de cada fluxo (REG)
        tcp.sendall(wire.enc_reg(topic, int(o["qos"])))
        r = rdr.read()
        if r is None or r[0] != wire.T_ACK or r[2][0] != 0:
            raise SystemExit("registo recusado: " + topic)

    pending = {}
    stop = threading.Event()

    def ack_loop():
        udp.settimeout(0.2)
        while not stop.is_set():
            try:
                data, _ = udp.recvfrom(65535)
            except socket.timeout:
                continue
            ack = wire.dec_udp_ack(data)
            if ack is None:
                continue
            pending.pop(ack, None)

    def resend_loop():
        while not stop.is_set():
            now = time.time()
            for tx_seq, state in list(pending.items()):
                if now - state["last"] >= ACK_TIMEOUT:
                    udp.sendto(state["pkt"], broker_data)
                    state["last"] = now
            time.sleep(0.05)

    threading.Thread(target=ack_loop, daemon=True).start()
    threading.Thread(target=resend_loop, daemon=True).start()

    # agenda: (instante relativo, topico, qos, msg_id)
    sched = []
    for topic, o in cfg.all("publish"):
        count, rate, qos = int(o["count"]), float(o["rate"]), int(o["qos"])
        sched += [(k / rate, topic, qos, k + 1) for k in range(count)]
    sched.sort()

    # tx_seq: contador do PUBLICADOR (um por salto publicador->broker), comum a
    # todos os fluxos. É ele que o ACK da fase 2 vai referir; o msg_id não serve
    # porque cada fluxo começa em 1.
    tx = 0
    time.sleep(start_after)
    t0 = time.time()
    for rel, topic, qos, mid in sched:
        wait = t0 + rel - time.time()
        if wait > 0:
            time.sleep(wait)
        tx = wire.seq_next(tx)
        # Regista ANTES de enviar: o instante de publicação nunca é posterior ao
        # de qualquer entrega (o relógio é em ms; registar depois do sendto deixa
        # o subscritor, noutro processo, registar a entrega 1 ms antes).
        log.write(topic, mid)
        pkt = wire.enc_data(qos, tx, mid, topic, b"synthetic")
        pending[tx] = {"pkt": pkt, "last": time.time()}
        try:
            udp.sendto(pkt, broker_data)
        except OSError as e:
            # A publicação existe (fica no registo); na fase 2 a retransmissão
            # volta a enviá-la. Aqui só não deixamos o processo morrer.
            print("[publisher %s] sendto: %r" % (ident, e), file=sys.stderr, flush=True)
    # Fase 2: o broker responde com ACK por UDP; se um datagrama não for
    # confirmado a tempo, o remetente retransmite até o ACK chegar. A espera final
    # também permite que o último ACK chegue antes do BYE.
    time.sleep(min(drain, LINGER))
    stop.set()
    try:
        tcp.sendall(wire.enc_frame(wire.T_BYE))
    except OSError:
        pass
    tcp.close()
