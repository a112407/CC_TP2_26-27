def valid_topic(t):
    p = t.split("/")
    return len(p) == 3 and all(p) and "*" not in t


def valid_filter(f):
    if valid_topic(f):
        return True
    if f.endswith("/*"):
        p = f[:-2].split("/")
        return 1 <= len(p) <= 2 and all(p) and "*" not in f[:-2]
    return False


def matches(flt, topic):
    if flt.endswith("/*"):
        return topic.startswith(flt[:-1])  # mantém a '/'
    return flt == topic


def candidate_filters(topic):
    """Os únicos 3 filtros que podem corresponder a 'regiao/estacao/metrica'
    (profundidade fixa): o tópico, 'regiao/estacao/*' e 'regiao/*'."""
    r, e, _ = topic.split("/")
    return (topic, "%s/%s/*" % (r, e), "%s/*" % r)
