#!/usr/bin/env python3
"""Verificação formal dos registos (secção 5.2 do enunciado). Só biblioteca padrão.

Confronta o registo de UM subscritor com o(s) registo(s) dos publicadores e
avalia seis critérios de aceitação. Termina com código 0 se todos passam.

    verify_fase1.py --pub pub1.log [--pub pub2.log ...] --sub sub1.log --sub-id sub1
                    [--filter 'north/*' ...] [--allow-duplicates]

--filter  filtros do subscritor, tal como na sua configuração (sem eles, espera-se
          tudo o que foi publicado). A correspondência usa src/topics.py, a mesma
          regra do broker, o que testa também a cópia única (secção 3.4).
"""
import argparse, os, re, statistics, sys, time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))
from topics import matches   # noqa: E402

# <timestamp_ms> <id> <topico> <message_id>: 13 dígitos = ms Unix até ao ano 2286
LINE = re.compile(r"^(\d{13}) (\S+) ([^/*\s]+/[^/*\s]+/[^/*\s]+) (\S+)$")


def load(path):
    """Devolve (entradas, linhas_invalidas, termina_em_newline)."""
    with open(path, "rb") as f:
        raw = f.read()
    entries, bad = [], []
    for n, line in enumerate(raw.decode("utf-8", "replace").split("\n"), 1):
        if not line:
            continue
        m = LINE.match(line)
        if m:
            entries.append((int(m[1]), m[2], m[3], m[4]))
        else:
            bad.append((n, line))
    return entries, bad, (not raw or raw.endswith(b"\n"))


def verify(pub_logs, sub_log, sub_id, filters=(), allow_dups=False, max_age_h=24, now_ms=None):
    """Lista de (criterio, passou, detalhe)."""
    now_ms = now_ms or int(time.time() * 1000)
    res = []
    pubs = [(p,) + load(p) for p in pub_logs]
    sub_entries, sub_bad, sub_nl = load(sub_log)

    # R1 formato: cada linha cumpre a gramática; ids coerentes; linha final completa
    bad = sum(len(b) for _, _, b, _ in pubs) + len(sub_bad)
    ids_ok = all(len({e[1] for e in ent}) <= 1 for _, ent, _, _ in pubs) \
        and all(e[1] == sub_id for e in sub_entries)
    nl_ok = sub_nl and all(nl for _, _, _, nl in pubs)
    total = sum(len(e) for _, e, _, _ in pubs) + len(sub_entries)
    res.append(("R1 formato '<ms> <id> <tópico> <message_id>'",
                bad == 0 and ids_ok and nl_ok,
                "%d linhas válidas, %d inválidas, id %s" % (total, bad, "coerente" if ids_ok else "INCOERENTE")))

    # R2 relógio: tempo Unix em ms (parede), não contador monotónico nem tempo desde o arranque
    stamps = [e[0] for _, ent, _, _ in pubs for e in ent] + [e[0] for e in sub_entries]
    lo, hi = now_ms - max_age_h * 3600 * 1000, now_ms + 60 * 1000
    fora = [t for t in stamps if not lo <= t <= hi]
    res.append(("R2 relógio de parede (ms Unix)", not fora and bool(stamps),
                "%d/%d dentro de [agora-%dh, agora+1min]" % (len(stamps) - len(fora), len(stamps), max_age_h)))

    # R3 identidade: o par <tópico, message_id> identifica a publicação nos dois registos
    pub_ts, pub_dup = {}, 0
    for _, ent, _, _ in pubs:
        for ts, _, topic, mid in ent:
            if (topic, mid) in pub_ts:
                pub_dup += 1
            pub_ts[(topic, mid)] = ts
    expected = {k for k in pub_ts if not filters or any(matches(f, k[0]) for f in filters)}
    received = {(e[2], e[3]) for e in sub_entries}
    faltam, sobram = sorted(expected - received), sorted(received - expected)
    res.append(("R3 completude e seletividade (esperado == recebido)",
                not faltam and not sobram and pub_dup == 0,
                "esperadas %d, recebidas %d, em falta %d, a mais %d%s"
                % (len(expected), len(received), len(faltam), len(sobram),
                   (" | ex. em falta: %s %s" % faltam[0]) if faltam else
                   (" | ex. a mais: %s %s" % sobram[0]) if sobram else "")))

    # R4 duplicados: G1 não elimina, mas sem retransmissão (ou perda) não pode haver
    from collections import Counter
    mult = Counter((e[2], e[3]) for e in sub_entries)
    dups = sum(c - 1 for c in mult.values() if c > 1)
    res.append(("R4 sem duplicados no registo" + (" (permitidos: G1 sob perda)" if allow_dups else ""),
                allow_dups or dups == 0,
                "%d entregas repetidas em %d publicações" % (dups, sum(1 for c in mult.values() if c > 1))))

    # R5 causalidade: nenhuma entrega antes da publicação (relógio comum à VM)
    first = {}
    for ts, _, topic, mid in sub_entries:
        first[(topic, mid)] = min(ts, first.get((topic, mid), ts))
    lat = [first[k] - pub_ts[k] for k in first if k in pub_ts]
    neg = sum(1 for x in lat if x < 0)
    res.append(("R5 causalidade (entrega >= publicação)", neg == 0,
                "%d violações em %d pares" % (neg, len(lat)) + ("" if lat else " (nada entregue: vacuoso)")))

    # R6 latência ponta a ponta (informativa; dá o valor de referência para a fase 2)
    if lat:
        s = sorted(lat)
        p95 = s[min(len(s) - 1, int(0.95 * len(s)))]
        res.append(("R6 latência pub->sub (informativa)", True,
                    "n=%d min=%d mediana=%.0f p95=%d max=%d ms" % (len(s), s[0], statistics.median(s), p95, s[-1])))
    return res


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--pub", action="append", required=True, help="registo de um publicador")
    ap.add_argument("--sub", required=True, help="registo do subscritor")
    ap.add_argument("--sub-id", required=True)
    ap.add_argument("--filter", action="append", default=[])
    ap.add_argument("--allow-duplicates", action="store_true")
    a = ap.parse_args(argv)
    res = verify(a.pub, a.sub, a.sub_id, a.filter, a.allow_duplicates)
    print("Subscritor %s | filtros: %s" % (a.sub_id, ", ".join(a.filter) or "(todos)"))
    for name, ok, detail in res:
        print("  [%s] %s: %s" % ("PASS" if ok else "FAIL", name, detail))
    ok = all(r[1] for r in res)
    print("RESULTADO: %s" % ("ACEITE" if ok else "REJEITADO"))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
