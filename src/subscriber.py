import socket, sys, threading, time
import wire
from logutil import DeliveryLog


def run_subscriber(cfg, ident):
    t_start = time.time()
    host, port = cfg.get_addr("broker_control")
    log = DeliveryLog(cfg.get("log"), ident)
    duration = cfg.get_float("duration")

    udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp.bind(("0.0.0.0", 0))
    tcp = socket.create_connection((host, port))
    rdr = wire.FrameReader(tcp)
    tcp.sendall(wire.enc_hello(wire.ROLE_SUB, udp.getsockname()[1], ident))
    m = rdr.read()
    if m is None or m[0] != wire.T_WELCOME:
        raise SystemExit("sessão recusada")
    data_port, _ = wire.dec_welcome(m[2])
    # Destino dos ACKs EventCast (fase 2). Têm de sair do mesmo socket 'udp',
    # cuja porta o broker guardou no HELLO.
    broker_data = (host, data_port)

    stop = threading.Event()

    def recv_loop():
        udp.settimeout(0.2)
        while not stop.is_set():
            try:
                data, _ = udp.recvfrom(65535)
            except socket.timeout:
                continue
            d = wire.dec_data(data)
            if d is None:
                continue
            qos, _, mid, topic, _ = d
            # fase 3: se qos == 2, consultar tabela de duplicados aqui,
            # com chave (topic, mid): o mid sozinho colide entre fluxos.
            log.write(topic, mid)

    threading.Thread(target=recv_loop, daemon=True).start()

    session_ok = True   # passa a False se o broker fechar a sessão (ex.: expulsão)

    def control(frame, what):
        """Pedido SubControl com resposta. Devolve True se aceite. Se a sessão
        caiu, deixa de tentar (o registo e a receção continuam até 'duration')."""
        nonlocal session_ok
        if not session_ok:
            return False
        try:
            tcp.sendall(frame)
            r = rdr.read()
        except OSError:        # BrokenPipeError, ConnectionResetError, ...
            r = None
        if r is None:
            session_ok = False
            print("[subscriber %s] sessão fechada pelo broker (%s)" % (ident, what),
                  file=sys.stderr, flush=True)
            return False
        if r[0] != wire.T_ACK or r[2][0] != 0:
            print("[subscriber %s] recusado: %s" % (ident, what), file=sys.stderr, flush=True)
            return False
        return True

    def subscribe(flt, qos):
        control(wire.enc_sub(flt, qos), "subscribe " + flt)

    def unsubscribe(flt):
        control(wire.enc_unsub(flt), "unsubscribe " + flt)

    for flt, o in cfg.all("subscribe"):
        subscribe(flt, int(o["qos"]))

    actions = []  # ações 'at', relativas ao arranque do processo
    for at, o in cfg.all("at"):
        if "subscribe" in o:
            actions.append((float(at), "sub", o["subscribe"], int(o["qos"])))
        elif "unsubscribe" in o:
            actions.append((float(at), "unsub", o["unsubscribe"], 0))
    actions.sort()
    for at, kind, flt, qos in actions:
        wait = t_start + at - time.time()
        if wait > 0:
            time.sleep(wait)
        subscribe(flt, qos) if kind == "sub" else unsubscribe(flt)

    rest = t_start + duration - time.time()
    if rest > 0:
        time.sleep(rest)
    stop.set()
    try:
        tcp.sendall(wire.enc_frame(wire.T_BYE))
    except OSError:
        pass
    tcp.close()
