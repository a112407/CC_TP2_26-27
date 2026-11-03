"""Testes da fase 1 (loopback). Só biblioteca padrão.

    python3 tests/test_fase1.py

Os testes 'crus' falam com o broker (lançado pelo run.sh) com sockets e wire.py,
o que os torna deterministas. O teste de contrato corre os três papéis pelo
run.sh a partir de '/'. A rede real (CORE, perda) NÃO é testada aqui.
"""
import os, socket, subprocess, sys, tempfile, threading, time, unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))
import wire                                     # noqa: E402
from broker import Broker                       # noqa: E402

RUN = os.path.join(ROOT, "run.sh")
_port = [18000]


def ports():
    _port[0] += 2
    return _port[0], _port[0] + 1


def write_cfg(path, text):
    with open(path, "w") as f:
        f.write(text)
    return path


class RawBroker(unittest.TestCase):
    """Um broker real (run.sh) por teste, clientes escritos à mão."""

    def setUp(self):
        self.cp, self.dp = ports()
        self.tmp = tempfile.mkdtemp()
        cfg = write_cfg(os.path.join(self.tmp, "b.conf"),
                        "bind = 127.0.0.1\ncontrol_port = %d\ndata_port = %d\n"
                        "duration = 30\nevict_after = 5\n" % (self.cp, self.dp))
        self.proc = subprocess.Popen([RUN, "broker", "--id", "b", "--config", cfg],
                                     cwd="/", stderr=subprocess.PIPE)
        time.sleep(0.5)
        self.socks = []

    def tearDown(self):
        for s in self.socks:
            s.close()
        self.proc.kill()
        self.proc.wait()
        self.proc.stderr.close()

    def alive(self):
        return self.proc.poll() is None

    def connect(self, role, ident, udp=None):
        u = udp or socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        if udp is None:
            u.bind(("127.0.0.1", 0))
        u.settimeout(0.4)
        t = socket.create_connection(("127.0.0.1", self.cp))
        t.settimeout(2)
        self.socks += [u, t]
        t.sendall(wire.enc_hello(role, u.getsockname()[1], ident))
        rd = wire.FrameReader(t)
        m = rd.read()
        return t, rd, u, wire.dec_welcome(m[2])[0]

    def ask(self, t, rd, frame):
        t.sendall(frame)
        r = rd.read()
        self.assertEqual(r[0], wire.T_ACK)
        return r[2][0]

    def pub(self, ident="p"):
        return self.connect(wire.ROLE_PUB, ident)

    def sub(self, ident="s"):
        return self.connect(wire.ROLE_SUB, ident)

    @staticmethod
    def got(u, wait=0.5):
        out, end = [], time.time() + wait
        while time.time() < end:
            try:
                d, _ = u.recvfrom(65535)
            except socket.timeout:
                break
            out.append(wire.dec_data(d))
        return out

    def send(self, u, dp, topic, mid=1, qos=1, tx=1):
        u.sendto(wire.enc_data(qos, tx, mid, topic, b"x"), ("127.0.0.1", dp))

    # ---- funcionalidade ----
    def test_filtros_sobrepostos_uma_copia_e_qos_efetivo(self):
        pt, pr, pu, dp = self.pub()
        self.assertEqual(self.ask(pt, pr, wire.enc_reg("north/station7/temperature", 2)), 0)
        st, sr, su, _ = self.sub()
        for f, q in (("north/*", 1), ("north/station7/*", 2), ("north/station7/temperature", 1)):
            self.assertEqual(self.ask(st, sr, wire.enc_sub(f, q)), 0)
        self.send(pu, dp, "north/station7/temperature", mid=7)
        r = self.got(su)
        self.assertEqual(len(r), 1, "3 filtros sobrepostos -> 1 cópia")
        self.assertEqual((r[0][0], r[0][2]), (2, 7))            # qos 2, msg_id 7

    def test_qos_do_fluxo_limita(self):
        pt, pr, pu, dp = self.pub()
        self.ask(pt, pr, wire.enc_reg("north/station7/temperature", 1))
        st, sr, su, _ = self.sub()
        self.ask(st, sr, wire.enc_sub("north/*", 2))
        self.send(pu, dp, "north/station7/temperature")
        self.assertEqual(self.got(su)[0][0], 1)                  # fluxo 1, subscrição 2 -> 1

    def test_seletividade_e_unsub_com_filtro_sobreposto(self):
        pt, pr, pu, dp = self.pub()
        self.ask(pt, pr, wire.enc_reg("north/station7/temperature", 1))
        self.ask(pt, pr, wire.enc_reg("north/station7/humidity", 1))
        st, sr, su, _ = self.sub("s1")
        qt, qr, qu, _ = self.sub("s2")
        self.ask(st, sr, wire.enc_sub("north/station7/temperature", 1))
        self.ask(st, sr, wire.enc_sub("north/*", 1))
        self.ask(qt, qr, wire.enc_sub("north/station7/humidity", 1))
        self.send(pu, dp, "north/station7/temperature", mid=1)
        self.send(pu, dp, "north/station7/humidity", mid=1, tx=2)
        self.assertEqual(sorted(x[3] for x in self.got(su)),
                         ["north/station7/humidity", "north/station7/temperature"])
        self.assertEqual([x[3] for x in self.got(qu)], ["north/station7/humidity"])
        # cancelar um filtro sobreposto não tira a entrega do outro
        self.assertEqual(self.ask(st, sr, wire.enc_unsub("north/station7/temperature")), 0)
        self.send(pu, dp, "north/station7/temperature", mid=2, tx=3)
        self.assertEqual(len(self.got(su)), 1)
        # cancelar o último: nenhum datagrama
        self.assertEqual(self.ask(st, sr, wire.enc_unsub("north/*")), 0)
        self.send(pu, dp, "north/station7/temperature", mid=3, tx=4)
        self.assertEqual(self.got(su), [])

    def test_fecho_de_sessao_remove_subscritor(self):
        pt, pr, pu, dp = self.pub()
        self.ask(pt, pr, wire.enc_reg("north/station7/temperature", 1))
        st, sr, su, _ = self.sub()
        self.ask(st, sr, wire.enc_sub("north/*", 1))
        st.close()
        time.sleep(0.3)
        self.send(pu, dp, "north/station7/temperature")
        self.assertEqual(self.got(su), [])
        self.assertTrue(self.alive())

    def test_sub_antes_do_reg(self):
        st, sr, su, _ = self.sub()
        self.ask(st, sr, wire.enc_sub("north/*", 1))
        pt, pr, pu, dp = self.pub()
        self.ask(pt, pr, wire.enc_reg("north/station7/temperature", 1))
        self.send(pu, dp, "north/station7/temperature")
        self.assertEqual(len(self.got(su)), 1)

    # ---- controlo: toda a mensagem recebe resposta ----
    def test_filtro_e_qos_invalidos(self):
        st, sr, _, _ = self.sub()
        self.assertEqual(self.ask(st, sr, wire.enc_sub("north/*/x", 1)), 1)
        self.assertEqual(self.ask(st, sr, wire.enc_sub("north/*", 3)), 1)
        pt, pr, _, _ = self.pub()
        self.assertEqual(self.ask(pt, pr, wire.enc_reg("north/*", 1)), 1)
        self.assertEqual(self.ask(pt, pr, wire.enc_reg("north/s/t", 1)), 0)

    def test_comando_fora_do_papel_e_tipo_desconhecido(self):
        pt, pr, _, _ = self.pub()
        self.assertEqual(self.ask(pt, pr, wire.enc_sub("north/*", 1)), 1)    # antes: sem resposta
        self.assertEqual(self.ask(pt, pr, wire.enc_unsub("north/*")), 1)
        self.assertEqual(self.ask(pt, pr, wire.enc_frame(99, b"?")), 1)
        st, sr, _, _ = self.sub()
        self.assertEqual(self.ask(st, sr, wire.enc_reg("x/y/z", 1)), 1)      # antes: aceite

    # ---- robustez: nada disto pode matar o broker ----
    def test_hello_com_porta_udp_zero_e_papel_invalido(self):
        for role, port in ((wire.ROLE_SUB, 0), (wire.ROLE_PUB, 0), (9, 5000)):
            t = socket.create_connection(("127.0.0.1", self.cp))
            t.settimeout(2)
            t.sendall(wire.enc_hello(role, port, "x"))
            self.assertIsNone(wire.FrameReader(t).read(), "sessão deve ser fechada")
            t.close()
        self.assertTrue(self.alive())

    def test_hello_malformado(self):
        for payload in (b"", b"\x02", b"\x02\x00\x01\x05ab", b"\x02\x13\x88\x02\xff\xfe"):
            t = socket.create_connection(("127.0.0.1", self.cp))
            t.settimeout(2)
            t.sendall(wire.enc_frame(wire.T_HELLO, payload))
            self.assertIsNone(wire.FrameReader(t).read())
            t.close()
        self.assertTrue(self.alive())

    def test_frame_truncado_e_ligacao_cortada(self):
        t = socket.create_connection(("127.0.0.1", self.cp))
        t.sendall(b"\x00\x10\x04")                  # cabeçalho a meio
        t.close()
        t = socket.create_connection(("127.0.0.1", self.cp))
        t.sendall(wire.enc_hello(wire.ROLE_SUB, 5000, "x")[:6])
        t.close()
        time.sleep(0.3)
        self.assertTrue(self.alive())

    def test_datagramas_maus_nao_matam_o_broker(self):
        pt, pr, pu, dp = self.pub()
        self.ask(pt, pr, wire.enc_reg("a/b/c", 1))
        st, sr, su, _ = self.sub()
        self.ask(st, sr, wire.enc_sub("a/*", 1))
        topic = b"a/b/" + b"\xff" * 96             # UTF-8 inválido: antes levantava OverflowError
        bad = bytes([1, 1]) + bytes(4) + (1).to_bytes(4, "big") + bytes([len(topic)]) + topic
        for d in (b"", b"\x01", bad, bytes([7]) * 30, wire.enc_data(1, 1, 1, "a/b/c")[:12]):
            pu.sendto(d, ("127.0.0.1", dp))
        time.sleep(0.3)
        self.assertTrue(self.alive())
        self.send(pu, dp, "a/b/c", mid=5)
        r = self.got(su)
        self.assertEqual([x[2] for x in r], [5], "só a publicação válida chega")

    def test_datagrama_de_origem_desconhecida_ou_topico_nao_registado(self):
        pt, pr, pu, dp = self.pub()
        self.ask(pt, pr, wire.enc_reg("a/b/c", 1))
        st, sr, su, _ = self.sub()
        self.ask(st, sr, wire.enc_sub("a/*", 1))
        intruso = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socks.append(intruso)
        self.send(intruso, dp, "a/b/c")             # origem sem sessão
        self.send(pu, dp, "a/b/outro")              # fluxo não registado
        self.send(su, dp, "a/b/c")                  # origem é um subscritor
        self.assertEqual(self.got(su), [])
        self.assertTrue(self.alive())


class UnitBroker(unittest.TestCase):
    def test_sendto_a_falhar_nao_propaga_e_nao_afeta_outros(self):
        class Cfg:
            def get(self, k): return "127.0.0.1"
            def get_int(self, k): return 1
            def get_float(self, k): return 1.0
        b = Broker(Cfg(), "u")
        enviados = []

        class FakeUdp:
            def sendto(self, d, a):
                if a[1] == 0:
                    raise OSError(101, "Network is unreachable")
                enviados.append(a)
        b.udp = FakeUdp()
        b.by_addr[("10.0.0.1", 9)] = 1
        b.pubs[1] = {"addr": ("10.0.0.1", 9), "tx": 0, "id": "p", "conn": None}
        b.subs[2] = {"addr": ("10.0.0.2", 0), "tx": 0, "id": "s2", "conn": None}   # falha
        b.subs[3] = {"addr": ("10.0.0.3", 5), "tx": 0, "id": "s3", "conn": None}   # ok
        b.table = {"a/*": {2: 1, 3: 1}}
        b.topics = {"a/b/c": {"qos": 1, "pub": "p", "subs": {2: 1, 3: 1}}}
        b.warn = lambda m: None
        b.on_data(wire.enc_data(1, 1, 1, "a/b/c"), ("10.0.0.1", 9))
        self.assertEqual(enviados, [("10.0.0.3", 5)])

    def test_tx_seq_por_subscritor_com_volta(self):
        self.assertEqual(wire.seq_next(0), 1)
        self.assertEqual(wire.seq_next(2**32 - 1), 1)            # salta o 0
        self.assertEqual(wire.enc_data(1, 2**32 + 3, 1, "a/b/c")[2:6], (3).to_bytes(4, "big"))
        self.assertEqual(wire.enc_udp_ack(2**32 + 3), bytes([2]) + (3).to_bytes(4, "big"))
        self.assertTrue(wire.seq_lt(2**32 - 2, 3))               # 2**32-2 vem antes de 3
        self.assertFalse(wire.seq_lt(3, 2**32 - 2))
        self.assertFalse(wire.seq_lt(7, 7))


class Contrato(unittest.TestCase):
    """Os três papéis pelo run.sh, a partir de '/', com registos conferidos."""

    def test_e2e_registos_iguais(self):
        cp, dp = ports()
        d = tempfile.mkdtemp()
        w = lambda n, t: write_cfg(os.path.join(d, n), t)
        b = w("b.conf", "bind = 127.0.0.1\ncontrol_port = %d\ndata_port = %d\n"
                        "duration = 7\nevict_after = 5\n" % (cp, dp))
        p = w("p.conf", "broker_control = 127.0.0.1:%d\nlog = %s/p.log\nstart_after = 1\ndrain = 2\n"
                        "publish = north/station7/temperature qos=1 count=30 rate=30\n"
                        "publish = north/station7/humidity qos=2 count=30 rate=30\n" % (cp, d))
        s1 = w("s1.conf", "broker_control = 127.0.0.1:%d\nlog = %s/s1.log\nduration = 4.5\n"
                          "subscribe = north/* qos=1\nsubscribe = north/station7/* qos=2\n"
                          "subscribe = north/station7/temperature qos=1\n" % (cp, d))
        s2 = w("s2.conf", "broker_control = 127.0.0.1:%d\nlog = %s/s2.log\nduration = 4.5\n"
                          "subscribe = north/station7/humidity qos=2\n" % (cp, d))
        procs = [subprocess.Popen([RUN, "broker", "--id", "b1", "--config", b], cwd="/")]
        time.sleep(0.5)
        procs += [subprocess.Popen([RUN, "subscriber", "--id", n, "--config", c], cwd="/")
                  for n, c in (("s1", s1), ("s2", s2))]
        time.sleep(0.3)
        procs.append(subprocess.Popen([RUN, "publisher", "--id", "p1", "--config", p], cwd="/"))
        for x in procs:
            self.assertEqual(x.wait(timeout=20), 0)

        def keys(n, col=(2, 3)):
            with open(os.path.join(d, n)) as f:
                return [tuple(l.split()[c] for c in col) for l in f]
        pub = keys("p.log")
        self.assertEqual(len(pub), 60)
        self.assertEqual(sorted(keys("s1.log")), sorted(pub))
        self.assertEqual(len(set(keys("s1.log"))), 60, "sem duplicados numa rede sem perdas")
        self.assertEqual(sorted(keys("s2.log")), sorted(k for k in pub if k[0].endswith("humidity")))
        with open(os.path.join(d, "s1.log")) as f:
            for linha in f:
                ts, ident, topic, mid = linha.split()
                self.assertEqual(ident, "s1")
                self.assertGreater(int(ts), 1_700_000_000_000)  # ms Unix, relógio de parede


class SubscritorExpulso(unittest.TestCase):
    def test_sessao_fechada_pelo_broker_nao_derruba_o_subscritor(self):
        """Broker de mentira: dá WELCOME e fecha a sessão (como uma expulsão).
        O subscritor tem de terminar limpo, sem traceback, com código 0."""
        srv = socket.socket()
        srv.bind(("127.0.0.1", 0))
        srv.listen(1)
        cp = srv.getsockname()[1]

        def broker_falso():
            c, _ = srv.accept()
            wire.FrameReader(c).read()                         # HELLO
            c.sendall(wire.enc_welcome(9999, 1))
            c.close()
        threading.Thread(target=broker_falso, daemon=True).start()
        d = tempfile.mkdtemp()
        cfg = write_cfg(os.path.join(d, "s.conf"),
                        "broker_control = 127.0.0.1:%d\nlog = %s/s.log\nduration = 2.5\n"
                        "subscribe = north/* qos=1\n"
                        "at = 1 subscribe=north/station7/* qos=2\n"
                        "at = 1.5 unsubscribe=north/*\n" % (cp, d))
        r = subprocess.run([RUN, "subscriber", "--id", "s", "--config", cfg], cwd="/",
                           capture_output=True, text=True, timeout=15)
        srv.close()
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertNotIn("Traceback", r.stderr)
        self.assertIn("sessão fechada", r.stderr)


if __name__ == "__main__":
    unittest.main(verbosity=2)
