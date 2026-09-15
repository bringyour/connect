import re,sys
provall=open(sys.argv[1]).read(); cli=open(sys.argv[2]).read(); prov=open(sys.argv[3]).read()
def xd(t):
    return {int(m.group(6)):(int(m.group(1)),int(m.group(2)),int(m.group(3)),int(m.group(4)),int(m.group(5))) for m in re.finditer(r'XDIAG h1 writes=(\d+) reads=(\d+) flushErrors=(\d+) writesLarge=(\d+) readsLarge=(\d+) unix=(\d+)',t)}
P=xd(provall); C=xd(cli)
# steady seconds: both sides carrying download data
secs=sorted(s for s in P if s in C and P[s][3]>5000 and C[s][4]>5000)
secs=secs[3:-3]
pw=sum(P[s][3] for s in secs); cr=sum(C[s][4] for s in secs); pf=sum(P[s][2] for s in secs)
# also allow +-1 s clock skew: compare totals over the span
span=range(secs[0],secs[-1]+1)
pw2=sum(P.get(s,(0,0,0,0,0))[3] for s in span); cr2=sum(C.get(s,(0,0,0,0,0))[4] for s in span)
print(f"common steady seconds={len(secs)} span={len(span)}")
print(f"provider large websocket writes total={pw2}  client large websocket reads total={cr2}  diff={pw2-cr2} ({100*(pw2-cr2)/max(pw2,1):.3f}%)  provider flushErrors={pf}")
# sequence-level for the same wall seconds is not timestamped; report per-second rates
rows=[tuple(int(x) for x in m.groups()) for m in re.finditer(r'TDIAG dest=\S+ loops=\d+ .*?resends=(\d+) selective=(\d+) writes=(\d+) writeErrs=(\d+)',prov)]
rows=[r for r in rows if r[2]>5000][3:-3]
if rows: print(f"provider SendSequence writes/s mean={sum(r[2] for r in rows)/len(rows):.0f} resends/s mean={sum(r[0] for r in rows)/len(rows):.0f}")
print(f"large ws writes/s mean={pw2/len(span):.0f}  large ws reads/s mean={cr2/len(span):.0f}")
