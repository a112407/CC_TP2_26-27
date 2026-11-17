class Config:
    def __init__(self, path):
        # Cada chave pode aparecer mais do que uma vez:
        # chave -> [(valor_principal, {opcoes})].
        self.items = {}
        with open(path, "r") as f:
            for raw in f:
                # Os comentários podem estar no fim da linha, por isso
                # limpamos essa parte antes de tentar ler a configuração.
                line = raw.split("#", 1)[0].strip()
                if not line or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                tokens = value.split()
                if not tokens:
                    continue

                # O primeiro termo é o valor principal; os seguintes são
                # opções no formato nome=valor.
                opts = {}
                for tok in tokens[1:]:
                    if "=" in tok:
                        n, v = tok.split("=", 1)
                        opts[n] = v
                self.items.setdefault(key.strip(), []).append((tokens[0], opts))

    def get(self, key):
        # Aqui esperamos uma chave única e devolvemos só o valor principal.
        return self.items[key][0][0]

    def get_int(self, key):
        return int(self.get(key))

    def get_float(self, key):
        return float(self.get(key))

    def get_addr(self, key):
        # Usamos o último ':' para não partir o endereço antes da porta.
        host, port = self.get(key).rsplit(":", 1)
        return host, int(port)

    def all(self, key):
        # Para chaves repetidas, devolvemos todos os valores pela ordem
        # em que aparecem no ficheiro.
        return self.items.get(key, [])
