import time


class DeliveryLog:
    """<timestamp_ms> <id> <tópico> <message_id>, flush a cada linha.

    Cada execução deve começar com um ficheiro limpo. Em Fase 1, a verificação
    assume um único ciclo de vida por registo; usar append acumulava linhas de
    execuções anteriores e fazia parecer duplicados e entregas fora de ordem.
    """

    def __init__(self, path, ident):
        self.f = open(path, "w", buffering=1)
        self.ident = ident

    def write(self, topic, msg_id):
        self.f.write("%d %s %s %s\n" % (int(time.time() * 1000), self.ident, topic, msg_id))
        self.f.flush()
