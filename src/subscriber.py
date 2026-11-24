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
    tcp.settimeout(10.0)
    rdr = wire.FrameReader(tcp)
    tcp.sendall(wire.enc_hello(wire.ROLE_SUB, udp.getsockname()[1], ident))
    m = rdr.read()
    if m is None or m[0] != wire.T_WELCOME:
        raise SystemExit("sessão recusada")
    data_port, _ = wire.dec_welcome(m[2])

    # Os ACKs têm de sair pelo mesmo socket UDP que foi anunciado no HELLO.
    # É assim que o broker sabe a que subscritor pertencem.
    broker_data = (host, data_port)

    stop = threading.Event()
    seen = set()   # (topic, mid) já registados; só é consultado em qos=2

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
            qos, tx, mid, topic, _ = d
            try:
                udp.sendto(wire.enc_udp_ack(tx), broker_data)
            except OSError:
                pass

            # G1 (qos=1): sem estado, uma repetição fica registada duas vezes.
            # G2 (qos=2): elimina duplicados. O ACK já foi enviado acima,
            # por isso o broker pára de retransmitir mesmo para um duplicado.
            if qos == 2:
                if (topic, mid) in seen:
                    continue
                seen.add((topic, mid))
            log.write(topic, mid)

    threading.Thread(target=recv_loop, daemon=True).start()

    # A receção UDP continua até ao fim da duração, mesmo que o TCP caia.
    session_ok = True

    def control(frame, what):
        # Cada comando TCP espera um ACK. Depois de a sessão cair, não vale a
        # pena continuar a enviar comandos, mas ainda podemos receber UDP.
        nonlocal session_ok
        if not session_ok:
            return False
        try:
            tcp.sendall(frame)
            r = rdr.read()
        except OSError:
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

    # As ações são guardadas e ordenadas porque podem aparecer fora de ordem
    # no ficheiro de configuração.
    actions = []
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

    # Mantemos o processo vivo até à duração configurada para continuar
    # a receber mensagens que possam chegar entretanto.
    rest = t_start + duration - time.time()
    if rest > 0:
        time.sleep(rest)
    stop.set()
    try:
        tcp.sendall(wire.enc_frame(wire.T_BYE))
    except OSError:
        pass
    tcp.close()
