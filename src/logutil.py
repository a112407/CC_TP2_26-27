import time


class DeliveryLog:
    def __init__(self, path, ident):
        # Cada execução começa com um log novo. Se usássemos append,
        # as linhas de execuções anteriores pareciam mensagens duplicadas.
        self.f = open(path, "w", buffering=1)
        self.ident = ident

    def write(self, topic, msg_id):
        self.f.write("%d %s %s %s\n" % (int(time.time() * 1000), self.ident, topic, msg_id))

        # O log fica disponível logo a seguir a cada entrega, sem esperar
        # que o processo termine.
        self.f.flush()
