import re,sys,statistics as st
f=sys.argv[1]; pfx=sys.argv[2]
out=open(f).read(); g={}
for m in re.finditer(pfx+r'(\d)-(\w+)-r(\d) bin=\S+ goodput=(\d+).*?stall=(\d+)',out):
    g.setdefault((m.group(1),m.group(2)),[]).append((int(m.group(3)),int(m.group(4)),int(m.group(5))))
arms=sorted({k[1] for k in g}, key=lambda a: ['BETA','UP','UPnb','MG','UPr','UPc','MGr','MGc'].index(a) if a in ['BETA','UP','UPnb','MG','UPr','UPc','MGr','MGc'] else 9)
for fl in sorted({k[0] for k in g}, reverse=True):
    base={}
    for a in arms:
        v=sorted(g.get((fl,a),[])); gs=[x[1] for x in v]
        if not gs: continue
        print(f"f{fl} {a:5s} {gs} median {st.median(gs):.0f} stalls {sum(x[2] for x in v)}")
