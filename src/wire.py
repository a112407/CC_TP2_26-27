SEQ_MOD = 1 << 32
SEQ_MASK = SEQ_MOD - 1
RTO = 0.25


def seq_next(n):
    return (n + 1) & SEQ_MASK


def seq_lt(a, b):
    d = (b - a) & SEQ_MASK
    return d != 0 and d < (SEQ_MOD >> 1)


T_REG, T_SUB, T_UNSUB, T_HELLO, T_WELCOME, T_ACK, T_BYE = 1, 2, 3, 4, 5, 6, 7
ROLE_PUB, ROLE_SUB, ROLE_BROKER = 1, 2, 3
CTRL_HEAD = 4
MAX_PAYLOAD = 65535


def be(value, n):
    return value.to_bytes(n, "big")


def rd(buf, off, n):
    return int.from_bytes(buf[off:off + n], "big")

def enc_frame(mtype, payload=b"", qos=0):
    if len(payload) > MAX_PAYLOAD:
        raise ValueError("payload demasiado grande")
    return be(len(payload), 2) + be(mtype, 1) + be(qos, 1) + payload


class FrameReader:
    # Lê uma mensagem TCP inteira, mesmo que chegue dividida em vários recv.

    def __init__(self, sock):
        self.sock = sock

    def _exact(self, n):
        buf = b""
        while len(buf) < n:
            chunk = self.sock.recv(n - len(buf))
            if not chunk:
                return None
            buf += chunk
        return buf

    def read(self):
        head = self._exact(CTRL_HEAD)
        if head is None:
            return None
        body = self._exact(rd(head, 0, 2))
        if body is None:
            return None
        return head[2], head[3], body


def enc_hello(role, udp_port, ident):
    b = ident.encode()
    return enc_frame(T_HELLO, be(role, 1) + be(udp_port, 2) + be(len(b), 1) + b)


def dec_hello(p):
    if len(p) < 4 or len(p) < 4 + p[3]:
        raise ValueError("HELLO truncado")
    return p[0], rd(p, 1, 2), p[4:4 + p[3]].decode()


def enc_welcome(data_port, session_id):
    return enc_frame(T_WELCOME, be(data_port, 2) + be(session_id, 4))


def dec_welcome(p):
    return rd(p, 0, 2), rd(p, 2, 4)


def enc_reg(topic, qos):
    return enc_frame(T_REG, topic.encode(), qos)


def enc_sub(flt, qos):
    return enc_frame(T_SUB, flt.encode(), qos)


def enc_unsub(flt):
    return enc_frame(T_UNSUB, flt.encode())


def enc_ack(status=0):
    return enc_frame(T_ACK, be(status, 1))


E_DATA, E_ACK = 1, 2
DATA_FIXED = 11


def enc_data(qos, tx_seq, msg_id, topic, content=b""):
    t = topic.encode()
    return (be(E_DATA, 1) + be(qos, 1) + be(tx_seq & SEQ_MASK, 4) + be(msg_id, 4)
            + be(len(t), 1) + t + content)


def dec_data(d):
    if len(d) < DATA_FIXED or d[0] != E_DATA:
        return None
    tl = d[10]
    if len(d) < DATA_FIXED + tl:
        return None
    try:
        topic = d[11:11 + tl].decode()
    except UnicodeDecodeError:
        # Um pacote com texto inválido é tratado como um pacote truncado.
        return None
    return d[1], rd(d, 2, 4), rd(d, 6, 4), topic, d[11 + tl:]


def enc_udp_ack(tx_seq):
    return be(E_ACK, 1) + be(tx_seq & SEQ_MASK, 4)


def dec_udp_ack(d):
    if len(d) < 5 or d[0] != E_ACK:
        return None
    return rd(d, 1, 4)
