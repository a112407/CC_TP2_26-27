"""Parser do ficheiro de configuração: 'chave = valor  # comentário'.
Valor = primeiro termo + opções nome=valor. Sem valores por omissão."""

class Config:
    def __init__(self, path):
        self.items = {}  # chave -> lista de (termo, {opcoes}) pela ordem de aparição
        with open(path, "r") as f:
            for raw in f:
                line = raw.split("#", 1)[0].strip()
                if not line or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                tokens = value.split()
                if not tokens:
                    continue
                opts = {}
                for tok in tokens[1:]:
                    if "=" in tok:
                        n, v = tok.split("=", 1)
                        opts[n] = v
                self.items.setdefault(key.strip(), []).append((tokens[0], opts))

    def get(self, key):
        """Chave que aparece exatamente uma vez: devolve o termo (str)."""
        return self.items[key][0][0]

    def get_int(self, key):
        return int(self.get(key))

    def get_float(self, key):
        return float(self.get(key))

    def get_addr(self, key):
        host, port = self.get(key).rsplit(":", 1)
        return host, int(port)

    def all(self, key):
        """Chave que aparece zero ou mais vezes: lista de (termo, opcoes)."""
        return self.items.get(key, [])
