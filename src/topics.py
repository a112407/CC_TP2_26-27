def valid_topic(t):
    p = t.split("/")
    return len(p) == 3 and all(p) and "*" not in t


def valid_filter(f):
    # Um filtro pode ser um tópico exato ou terminar em /* para apanhar
    # todos os tópicos de uma região ou de uma estação.
    if valid_topic(f):
        return True
    if f.endswith("/*"):
        p = f[:-2].split("/")
        return 1 <= len(p) <= 2 and all(p) and "*" not in f[:-2]
    return False


def matches(flt, topic):
    if flt.endswith("/*"):
        # Tirar só o '*' mantém a '/' e evita confundir "norte/a" com
        # "norte/ab".
        return topic.startswith(flt[:-1])
    return flt == topic


def candidate_filters(topic):
    # Como os tópicos têm profundidade fixa, só estes três filtros podem
    # corresponder: o exato, a estação inteira ou a região inteira.
    r, e, _ = topic.split("/")
    return (topic, "%s/%s/*" % (r, e), "%s/*" % r)
