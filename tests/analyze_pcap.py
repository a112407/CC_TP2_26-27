#!/usr/bin/env python3
"""Análise de uma captura (.pcap) do sistema: descodifica o SubControl (TCP) e o
EventCast (UDP) a partir dos bytes e verifica requisitos do enunciado. Só
biblioteca padrão; os formatos são os de src/wire.py, reescritos aqui de
propósito: a captura é verificada independentemente do código que a produziu.

    tcpdump -i <iface> -nn -s0 -w L1.pcap 'tcp port 7310 or udp'
    analyze_pcap.py L1.pcap --control-port 7310 [--expect-count sub3=0 ...]
                          [--expect-qos sub1/north/station7/humidity=2 ...]
                          [--min-inflight N] [--expect-retx]
                          [--evict-after S --evicted sub1 [--tol 1.0]]

Critérios sempre avaliados: S1-S6 (sessão, porta, cabeçalhos, cópia única, qos) e
Q1-Q4 (números de sequência: tx_seq e msg_id arrancam em 0 e crescem de 1 em 1).
Opcionais: Q5 pipelining (--min-inflight), Q6 retransmissão concorrente com o envio
(--expect-retx), E1-E3 expulsão (--evict-after/--evicted).

Capturem com tcpdump (formato pcap). Se usaram o Wireshark (pcapng):
    editcap -F pcap in.pcapng out.pcap
"""
import argparse, collections, struct, sys

CTRL = {1: "REG", 2: "SUB", 3: "UNSUB", 4: "HELLO", 5: "WELCOME", 6: "ACK", 7: "BYE"}
ROLE = {1: "publicador", 2: "subscritor", 3: "broker"}
BE = lambda b: int.from_bytes(b, "big")


# ---------------------------------------------------------------- pcap -> segmentos
def read_pcap(path):
    with open(path, "rb") as f:
        d = f.read()
    m = d[:4]
    if m == b"\x0a\x0d\x0d\x0a":
        raise SystemExit("ficheiro pcapng: converter com 'editcap -F pcap in out'")
    if m in (b"\xd4\xc3\xb2\xa1", b"\x4d\x3c\xb2\xa1"):
        e, div = "<", (1e9 if m[0] == 0x4d else 1e6)
    elif m in (b"\xa1\xb2\xc3\xd4", b"\xa1\xb2\x3c\x4d"):
        e, div = ">", (1e9 if m[3] == 0x4d else 1e6)
    else:
        raise SystemExit("não é um ficheiro pcap")
    link = struct.unpack(e + "I", d[20:24])[0]
    pos = 24
    while pos + 16 <= len(d):
        sec, frac, incl, _ = struct.unpack(e + "IIII", d[pos:pos + 16])
        yield sec + frac / div, link, d[pos + 16:pos + 16 + incl]
        pos += 16 + incl


def ip_of(link, fr):
    """Extrai o pacote IPv4 de uma trama (Ethernet, Linux cooked v1/v2, IP cru)."""
    if link == 1:
        et, off = BE(fr[12:14]), 14
        while et in (0x8100, 0x88A8):
            et, off = BE(fr[off + 2:off + 4]), off + 4
        return fr[off:] if et == 0x0800 else None
    if link == 113:
        return fr[16:] if BE(fr[14:16]) == 0x0800 else None
    if link == 276:
        return fr[20:] if BE(fr[0:2]) == 0x0800 else None
    if link in (101, 12):
        return fr if fr and fr[0] >> 4 == 4 else None
    raise SystemExit("tipo de ligação %d não suportado" % link)


def segments(path):
    """(ts, proto, src, sport, dst, dport, flags, seq, payload)"""
    for ts, link, fr in read_pcap(path):
        ip = ip_of(link, fr)
        if not ip or len(ip) < 20:
            continue
        ihl, tot, proto = (ip[0] & 15) * 4, BE(ip[2:4]), ip[9]
        if BE(ip[6:8]) & 0x3FFF:                       # fragmento: fora do âmbito
            continue
        src = ".".join(map(str, ip[12:16])); dst = ".".join(map(str, ip[16:20]))
        seg = ip[ihl:tot]                              # tot: corta o enchimento da trama
        if proto == 6 and len(seg) >= 20:
            off = (seg[12] >> 4) * 4
            yield ts, "tcp", src, BE(seg[0:2]), dst, BE(seg[2:4]), seg[13], BE(seg[4:8]), seg[off:]
        elif proto == 17 and len(seg) >= 8:
            yield ts, "udp", src, BE(seg[0:2]), dst, BE(seg[2:4]), 0, 0, seg[8:BE(seg[4:6])]


# ---------------------------------------------------------------- SubControl
def frames(pkts):
    """Reconstrói a stream (por número de sequência TCP) e delimita as mensagens
    SubControl: 4 B de cabeçalho (Tam. Msg 2 B só do payload | Tipo | QoS) + payload.
    Devolve [(ts, tipo, qos, payload)], com o ts do pacote que completou a mensagem."""
    buf, marks, nxt = bytearray(), [], None
    for seq, ts, pl in sorted(pkts, key=lambda x: (x[0], x[1])):
        if not pl:
            continue
        nxt = seq if nxt is None else nxt
        if seq + len(pl) <= nxt:
            continue                                   # retransmissão TCP
        if seq > nxt:
            break                                      # lacuna: stream incompleta
        buf += pl[nxt - seq:]
        nxt = seq + len(pl)
        marks.append((len(buf), ts))
    out, o = [], 0
    while o + 4 <= len(buf):
        end = o + 4 + BE(buf[o:o + 2])
        if end > len(buf):
            break
        ts = next(t for n, t in marks if n >= end)
        out.append((ts, buf[o + 2], buf[o + 3], bytes(buf[o + 4:end])))
        o = end
    return out


class Client:
    def __init__(self, ip, port):
        self.ip, self.port = ip, port
        self.id = self.role = self.udp_port = self.data_port = self.session = None
        self.t_welcome = None
        self.cmds = []                                 # (tipo, texto, t_ack, status)
        self.first_udp = None

    @property
    def ep(self):
        return (self.ip, self.udp_port)


def sessions(path, cport):
    syn, c2b, b2c, broker_ip = {}, collections.defaultdict(list), collections.defaultdict(list), None
    for ts, pr, s, sp, d, dp, fl, seq, pl in segments(path):
        if pr != "tcp":
            continue
        if dp == cport:                                # cliente -> broker
            broker_ip = d
            c2b[(s, sp)].append((seq, ts, pl))
        elif sp == cport:                              # broker -> cliente
            b2c[(d, dp)].append((seq, ts, pl))
    clients = []
    for key in sorted(c2b):
        up, down = frames(c2b[key]), frames(b2c.get(key, []))
        c = Client(*key)
        for ts, t, q, p in up:
            if t == 4 and len(p) >= 4 and len(p) >= 4 + p[3]:
                c.role, c.udp_port, c.id = p[0], BE(p[1:3]), p[4:4 + p[3]].decode(errors="replace")
        for ts, t, q, p in down:
            if t == 5 and len(p) >= 6:
                c.data_port, c.session, c.t_welcome = BE(p[0:2]), BE(p[2:6]), ts
        acks = [(ts, p[0]) for ts, t, q, p in down if t == 6 and p]
        cmds = [(t, p.decode(errors="replace"), q) for ts, t, q, p in up if t in (1, 2, 3)]
        for i, (t, txt, q) in enumerate(cmds):         # cada comando recebe uma resposta, por ordem
            c.cmds.append((t, txt, q) + (acks[i] if i < len(acks) else (None, None)))
        if c.id is not None:
            clients.append(c)
    return clients, broker_ip


# ---------------------------------------------------------------- EventCast
def dec_dgram(pl):
    """('DATA', qos, tx_seq, msg_id, topico) | ('ACK', tx_seq) | None se malformado"""
    if pl[:1] == b"\x01" and len(pl) >= 11 and len(pl) >= 11 + pl[10]:
        try:
            return ("DATA", pl[1], BE(pl[2:6]), BE(pl[6:10]), pl[11:11 + pl[10]].decode())
        except UnicodeDecodeError:
            return None
    if pl[:1] == b"\x02" and len(pl) == 5:
        return ("ACK", BE(pl[1:5]))
    return None


def analyse(path, cport, expect_count=(), expect_qos=(), min_inflight=0, expect_retx=False,
            evict_after=None, evicted=None, tol=1.0):
    clients, broker_ip = sessions(path, cport)
    out, ok_all = [], True

    def check(name, ok, detail):
        nonlocal ok_all
        ok_all &= bool(ok)
        out.append("  [%s] %s: %s" % ("PASS" if ok else "FAIL", name, detail))

    if not clients:
        print("Nenhuma sessão SubControl na captura (porta %d)." % cport)
        return 1
    bports = {c.data_port for c in clients if c.data_port}
    by_ep = {c.ep: c for c in clients if c.udp_port}
    bep = [(broker_ip, p) for p in bports]

    deliveries = collections.defaultdict(list)         # id do subscritor -> [(ts, qos, tx, msg, topico)]
    pubs_data = collections.defaultdict(list)
    n_ack = n_bad = n_other = n_wrong = 0
    fins = []                                          # FIN/RST do broker: (ts, ip, porta do cliente)
    pub_acks = collections.defaultdict(dict)           # id publicador -> {tx_seq: ts do 1.º ACK}
    cli_acks = collections.defaultdict(list)           # id subscritor -> [ts dos ACK de entrega]
    for ts, pr, s, sp, d, dp, fl, seq, pl in segments(path):
        if pr == "tcp":
            if sp == cport and fl & 0x05:
                fins.append((ts, d, dp))
            continue
        src, dst = (s, sp), (d, dp)
        cli = by_ep.get(src) or by_ep.get(dst)
        if cli is None:
            n_other += 1
            continue
        if src not in bep and dst not in bep:          # cliente a falar UDP com quem não é o broker
            n_wrong += 1
            continue
        if cli.first_udp is None or ts < cli.first_udp:
            cli.first_udp = ts
        m = dec_dgram(pl)
        if m is None:
            n_bad += 1
        elif m[0] == "ACK":
            n_ack += 1
            if src in bep:
                pub_acks[cli.id].setdefault(m[1], ts)
            else:
                cli_acks[cli.id].append(ts)
        elif src in bep:                               # entrega do broker
            deliveries[cli.id].append((ts,) + m[1:])
        else:                                          # publicação do cliente
            pubs_data[cli.id].append((ts,) + m[1:])

    out.append("Sessões SubControl (porta %d):" % cport)
    for c in clients:
        out.append("  %-8s %-10s %s:%d  UDP anunciado %d  porta de dados (WELCOME) %s  comandos: %s"
                   % (c.id, ROLE.get(c.role, c.role), c.ip, c.port, c.udp_port, c.data_port,
                      ", ".join("%s %s" % (CTRL[t], x) for t, x, *_ in c.cmds) or "-"))
    out.append("Critérios:")
    # S1 a sessão SubControl precede o primeiro datagrama de cada cliente
    for c in clients:
        if c.first_udp is None:
            check("S1 %s sessão antes do 1.º datagrama" % c.id, True, "sem datagramas (n/a)")
        else:
            check("S1 %s sessão antes do 1.º datagrama" % c.id,
                  c.t_welcome is not None and c.t_welcome < c.first_udp,
                  "WELCOME em t=%.6f, 1.º UDP em t=%.6f (+%.3f ms)"
                  % (c.t_welcome or 0, c.first_udp, 1000 * (c.first_udp - (c.t_welcome or 0))))
    # S2 um subscritor só recebe depois do ACK da sua primeira subscrição
    for c in clients:
        if c.role == 2 and c.first_udp is not None:
            t_ack = next((a for t, x, q, a, st in c.cmds if t == 2 and a), None)
            check("S2 %s primeira entrega depois do ACK do SUB" % c.id,
                  t_ack is not None and c.first_udp > t_ack,
                  "ACK do SUB em t=%.6f, 1.ª entrega em t=%.6f" % (t_ack or 0, c.first_udp))
    # S3 a porta de dados usada é a anunciada no WELCOME (não fixa no código)
    check("S3 porta de dados usada = a anunciada no WELCOME", len(bports) == 1 and n_wrong == 0,
          "WELCOME anuncia %s; %d datagramas dos clientes para outros destinos" % (sorted(bports), n_wrong))
    # S4 cabeçalhos EventCast bem formados
    total = sum(len(v) for v in deliveries.values()) + sum(len(v) for v in pubs_data.values())
    check("S4 cabeçalhos EventCast bem formados", n_bad == 0,
          "%d DATA, %d ACK, %d malformados, %d UDP fora do sistema ignorados" % (total, n_ack, n_bad, n_other))
    # S5 cópia única: um datagrama por (subscritor, publicação) na rede
    for cid, dl in sorted(deliveries.items()):
        # cópia única: um só tx_seq por (tópico, msg). Repetir o MESMO tx_seq é retransmissão
        # (fiabilidade, permitido); um tx_seq novo para a mesma publicação é uma 2.ª cópia.
        txs = collections.defaultdict(set)
        for _, q, tx, m, t in dl:
            txs[(t, m)].add(tx)
        copias = sum(len(v) - 1 for v in txs.values())
        retx = len(dl) - sum(len(v) for v in txs.values())
        check("S5 %s uma cópia por publicação" % cid, copias == 0,
              "%d DATA, %d publicações distintas, %d retransmissões (mesmo tx_seq), %d 2.ªs cópias (tx_seq novo)"
              % (len(dl), len(txs), retx, copias))
    # S6 qos efetivo por (subscritor, tópico)
    qos = collections.defaultdict(set)
    for cid, dl in deliveries.items():
        for _, q, tx, m, t in dl:
            qos[(cid, t)].add(q)
    out.append("DATA por subscritor e tópico:")
    for (cid, t), qs in sorted(qos.items()):
        n = sum(1 for _, q, tx, m, tt in deliveries[cid] if tt == t)
        out.append("    %-8s %-28s %5d datagramas, qos %s" % (cid, t, n, sorted(qs)))
    for spec in expect_count:                          # ID=N
        cid, n = spec.split("=")
        got = len(deliveries.get(cid, []))
        check("E %s recebeu %s datagramas DATA" % (cid, n), got == int(n), "observado %d" % got)
    for spec in expect_qos:                            # ID/TOPICO=Q
        left, q = spec.split("=")
        cid, topic = left.split("/", 1)
        check("E qos efetivo de %s em %s = %s" % (cid, topic, q),
              qos.get((cid, topic)) == {int(q)}, "observado %s" % sorted(qos.get((cid, topic), [])))
    # Q: números de sequência (tx_seq e msg_id) e fiabilidade, por troço
    def seq_checks(label, dl, publisher_id=None):
        first, ident, order, mids, retx_ts, new_ts, same = {}, {}, [], collections.defaultdict(list), [], [], True
        for ts, q, tx, m, t in dl:
            if tx in first:
                retx_ts.append(ts)
                same &= ident[tx] == (m, t)
            else:
                first[tx], ident[tx] = ts, (m, t)
                order.append(tx); new_ts.append(ts); mids[t].append(m)
        check("Q1 %s 1.º tx_seq == 0" % label, order[0] == 0, "1.º = %d" % order[0])
        check("Q2 %s tx_seq novos consecutivos (+1)" % label,
              order == list(range(order[0], order[0] + len(order))), "%d distintos" % len(order))
        check("Q3 %s retransmissão repete o mesmo msg_id" % label, same, "%d retransmissões" % len(retx_ts))
        if publisher_id is None:
            return
        for t, l in sorted(mids.items()):
            check("Q4 %s msg_id de %s: 1.º == 0 e consecutivos" % (label, t),
                  sorted(l) == list(range(len(l))), "%d ids" % len(l))
        acked = pub_acks[publisher_id]
        evs = sorted([(first[t], 1) for t in first] + [(acked[t], -1) for t in first if t in acked])
        cur = mx = 0
        for _, dlt in evs:
            cur += dlt
            mx = max(mx, cur)
        if min_inflight:
            check("Q5 %s pipelining (máx. DATA por confirmar em simultâneo)" % label, mx >= min_inflight,
                  "máx = %d (mínimo %d)" % (mx, min_inflight))
        else:
            out.append("  [info] Q5 %s máx. DATA por confirmar em simultâneo = %d" % (label, mx))
        if expect_retx:
            inter = [t for t in retx_ts if t < new_ts[-1]]
            check("Q6 %s retransmissões concorrentes com o envio de novos" % label, bool(inter),
                  "%d de %d retransmissões antes do último envio novo" % (len(inter), len(retx_ts)))

    for cid, dl in sorted(pubs_data.items()):
        seq_checks("%s->broker" % cid, dl, publisher_id=cid)
    for cid, dl in sorted(deliveries.items()):
        seq_checks("broker->%s" % cid, dl)

    if evicted:
        c = next((c for c in clients if c.id == evicted), None)
        check("E0 sessão %s existe na captura" % evicted, c is not None, "")
        if c is not None:
            tf = min((t for t, ip, p in fins if (ip, p) == (c.ip, c.port)), default=None)
            acks = [t for t in cli_acks[c.id] if tf is None or t <= tf]
            check("E1 broker fechou a sessão TCP de %s" % evicted, tf is not None, "FIN/RST em t=%s" % tf)
            if tf is not None and acks:
                gap = tf - max(acks)
                check("E2 fecho entre evict_after e evict_after+tol", evict_after <= gap <= evict_after + tol,
                      "%.2f s após o último ACK (evict_after=%s)" % (gap, evict_after))
                late = [t for t, *_ in deliveries[c.id] if t > tf + 0.5]
                check("E3 sem DATA para %s depois do fecho" % evicted, not late, "%d datagramas tardios" % len(late))
    print("\n".join(out))
    print("RESULTADO: %s" % ("ACEITE" if ok_all else "REJEITADO"))
    return 0 if ok_all else 1


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("pcap")
    ap.add_argument("--control-port", type=int, required=True)
    ap.add_argument("--expect-count", action="append", default=[], metavar="ID=N")
    ap.add_argument("--expect-qos", action="append", default=[], metavar="ID/TOPICO=Q")
    ap.add_argument("--min-inflight", type=int, default=0, help="Q5: mínimo de DATA em trânsito")
    ap.add_argument("--expect-retx", action="store_true", help="Q6: exige retransmissões durante o envio")
    ap.add_argument("--evict-after", type=float, help="E2: evict_after do broker (s)")
    ap.add_argument("--evicted", metavar="ID", help="E1-E3: subscritor que deve ser expulso")
    ap.add_argument("--tol", type=float, default=1.0, help="E2: tolerância (s)")
    a = ap.parse_args()
    if a.evicted and a.evict_after is None:
        ap.error("--evicted exige --evict-after")
    sys.exit(analyse(a.pcap, a.control_port, a.expect_count, a.expect_qos, a.min_inflight,
                     a.expect_retx, a.evict_after, a.evicted, a.tol))