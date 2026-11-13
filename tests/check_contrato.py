#!/usr/bin/env python3
"""Verificação do contrato de execução (secção 5.2) e das restrições da secção
3.3. Só biblioteca padrão. Estática (run.sh, código) e dinâmica (executa o broker
pelo run.sh a partir de outra diretoria).

    python3 tests/check_contrato.py
"""
import ast, os, re, signal, socket, subprocess, sys, tempfile, time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = os.path.join(ROOT, "src")
RUN = os.path.join(ROOT, "run.sh")
# secção 3.3: serialização, pub/sub e fiabilidade sobre UDP proibidas; struct é
# proibido por decisão do grupo (int.to_bytes/from_bytes, sem dúvidas)
PROIBIDOS = {"pickle", "cPickle", "marshal", "shelve", "json", "simplejson", "yaml", "struct",
             "msgpack", "google", "protobuf", "paho", "zmq", "pika", "kafka", "amqp", "enet",
             "twisted", "xmlrpc", "plistlib"}
res = []


def check(name, ok, detail=""):
    res.append(ok)
    print("  [%s] %s%s" % ("PASS" if ok else "FAIL", name, (": " + detail) if detail else ""))


def free_port():
    s = socket.socket(); s.bind(("127.0.0.1", 0)); p = s.getsockname()[1]; s.close()
    return p


def estatico():
    print("Estático")
    raw = open(RUN, "rb").read()
    check("C1 run.sh com bit de execução", os.access(RUN, os.X_OK))
    check("C2 run.sh com terminações LF (sem CR)", b"\r" not in raw)
    check("C3 run.sh começa por interpretador (#!)", raw.startswith(b"#!"), raw.split(b"\n")[0].decode())
    txt = raw.decode()
    check("C4 run.sh resolve o seu caminho e usa exec",
          'dirname "$0"' in txt and re.search(r'^exec python3 "\$here/src/main\.py" "\$@"', txt, re.M) is not None)
    check("C5 run.sh sem setsid/nohup/disown/daemon nem processos em fundo",
          not re.search(r"setsid|nohup|disown|daemon|&\s*$", txt, re.M))
    mods, ints, ips = set(), [], []
    local = {f[:-3] for f in os.listdir(SRC) if f.endswith(".py")}
    for f in sorted(os.listdir(SRC)):
        if not f.endswith(".py"):
            continue
        tree = ast.parse(open(os.path.join(SRC, f)).read())
        for n in ast.walk(tree):
            if isinstance(n, ast.Import):
                mods |= {a.name.split(".")[0] for a in n.names}
            elif isinstance(n, ast.ImportFrom) and n.module:
                mods.add(n.module.split(".")[0])
            elif isinstance(n, ast.Constant):
                if isinstance(n.value, int) and not isinstance(n.value, bool) and 1024 <= n.value <= 65534:
                    ints.append((f, n.lineno, n.value))
                if isinstance(n.value, str) and re.fullmatch(r"\d{1,3}(\.\d{1,3}){3}", n.value) \
                        and n.value != "0.0.0.0":
                    ips.append((f, n.lineno, n.value))
    check("C6 só biblioteca padrão ou módulos do próprio projeto",
          all(m in sys.stdlib_module_names or m in local for m in mods), ", ".join(sorted(mods)))
    check("C7 nenhuma biblioteca proibida (serialização, pub/sub, fiabilidade UDP)",
          not (mods & PROIBIDOS), ", ".join(sorted(mods & PROIBIDOS)) or "nenhuma")
    check("C8 sem portas fixas no código (inteiros em 1024..65534)", not ints, str(ints[:3]) if ints else "nenhum")
    check("C9 sem endereços IPv4 fixos no código (exceto o curinga 0.0.0.0)", not ips, str(ips[:3]) if ips else "nenhum")


def dinamico():
    print("Dinâmico (executa o run.sh a partir de '/')")
    d = tempfile.mkdtemp()
    cfg = os.path.join(d, "b.conf")

    def escreve(dur):
        with open(cfg, "w") as f:    # inclui um comentário no fim da linha (formato da secção 5.2)
            f.write("bind = 127.0.0.1\ncontrol_port = %d # plano de controlo\ndata_port = %d\n"
                    "duration = %s\nevict_after = 5\n" % (free_port(), free_port(), dur))

    escreve(2)
    t0 = time.time()
    p = subprocess.Popen([RUN, "broker", "--id", "b1", "--config", cfg], cwd="/", preexec_fn=os.setpgrp,
                         stderr=subprocess.PIPE)
    rc = p.wait(timeout=15); dt = time.time() - t0
    check("C10 arranca a partir de '/' e termina sozinho em duration", rc == 0 and 1.8 <= dt <= 4.5,
          "código %s, %.2f s para duration=2" % (rc, dt))
    try:
        os.killpg(p.pid, 0); vivo = True
    except ProcessLookupError:
        vivo = False
    check("C11 nenhum processo sobra no grupo (nada de daemons)", not vivo)
    escreve(60)
    p = subprocess.Popen([RUN, "broker", "--id", "b1", "--config", cfg], cwd="/", preexec_fn=os.setpgrp,
                         stderr=subprocess.PIPE)
    time.sleep(1.0)
    t0 = time.time(); os.killpg(p.pid, signal.SIGTERM)
    try:
        p.wait(timeout=5); ok = True
    except subprocess.TimeoutExpired:
        ok = False; os.killpg(p.pid, signal.SIGKILL)
    check("C12 termina com SIGTERM ao grupo em menos de 5 s", ok, "%.2f s" % (time.time() - t0))
    try:
        os.killpg(p.pid, 0); vivo = True
    except ProcessLookupError:
        vivo = False
    check("C13 SIGTERM ao grupo não deixa processos", not vivo)
    p.stderr.close()
    # config com valores errados falha com mensagem, não com 'valores por omissão'
    bad = os.path.join(d, "bad.conf")
    open(bad, "w").write("bind = 127.0.0.1\n")
    r = subprocess.run([RUN, "broker", "--id", "b1", "--config", bad], cwd="/", capture_output=True, timeout=10)
    check("C14 sem valores por omissão: chave em falta recusa arrancar", r.returncode != 0)


if __name__ == "__main__":
    estatico(); dinamico()
    print("RESULTADO: %s" % ("ACEITE" if all(res) else "REJEITADO"))
    sys.exit(0 if all(res) else 1)
