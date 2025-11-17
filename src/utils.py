# utilitaires communs
def parse_ttl_line(line):
    # Le format TTL peut être complexe (<> <> <> .). On extrait 2 premiers tokens "src" "dst" simplifié.
    parts = line.strip().split()
    if len(parts) < 3:
        return None
    # retourne en l'état les URIs (ou cleans si nécessaire)
    src = parts[0]
    dst = parts[2]
    # ignore les bnodes et triples non-URI (tu peux affiner)
    if src.startswith("<") and dst.startswith("<"):
        return (src, dst)
    return None
