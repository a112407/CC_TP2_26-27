import time


class DeliveryLog:
    """<timestamp_ms> <id> <topico> <message_id>, flush a cada linha."""

    def __init__(self, path, ident):
        self.f = open(path, "a", buffering=1)
        self.ident = ident

    def write(self, topic, msg_id):
        self.f.write("%d %s %s %s\n" % (int(time.time() * 1000), self.ident, topic, msg_id))
        self.f.flush()
