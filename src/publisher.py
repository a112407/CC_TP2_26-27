import socket, time
import wire
from logutil import DeliveryLog


def run_publisher(cfg, ident):
    host, port = cfg.get_addr("broker_control")
    log = DeliveryLog(cfg.get("log"), ident)
    start_after = cfg.get_float("start_after")

    udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp.bind(("0.0.0.0", 0))
    tcp = socket.create_connection((host, port))
    tcp.sendall(wire.enc_hello(wire.ROLE_PUB, udp.getsockname()[1], ident))
    m = wire.FrameReader(tcp).read()
    if m is None or m[0] != wire.T_WELCOME:
        raise SystemExit("sessão recusada")
    data_port, _ = wire.dec_welcome(m[2])
    rdr = wire.FrameReader(tcp)
    for topic, o in cfg.all("publish"):      # registo de cada fluxo (REG)
        tcp.sendall(wire.enc_reg(topic, int(o["qos"])))
        r = rdr.read()
        if r is None or r[0] != wire.T_ACK or r[2][0] != 0:
            raise SystemExit("registo recusado: " + topic)

    # agenda: (instante relativo, topico, qos, msg_id)
    sched = []
    for topic, o in cfg.all("publish"):
        count, rate, qos = int(o["count"]), float(o["rate"]), int(o["qos"])
        sched += [(k / rate, topic, qos, k + 1) for k in range(count)]
    sched.sort()

    time.sleep(start_after)
    t0 = time.time()
    for rel, topic, qos, mid in sched:
        wait = t0 + rel - time.time()
        if wait > 0:
            time.sleep(wait)
        udp.sendto(wire.enc_data(qos, 0, mid, topic, b"synthetic"), (host, data_port))
        log.write(topic, mid)
    # TODO fase 2: retransmitir até 'drain' segundos / tudo confirmado
    tcp.sendall(wire.enc_frame(wire.T_BYE))
    tcp.close()
