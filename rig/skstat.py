import re,sys,statistics as st,glob,os
def stat(L):
    prov=open(f'td-{L}.txt').read(); cli=open(f'cl-{L}.txt').read() if os.path.exists(f'cl-{L}.txt') else ''
    P={}
    for m in re.finditer(r'TDIAG dest=(\S+) loops=\d+ noCap=([\d.]+)% otherNil=[\d.]+% starve=([\d.]+)% work=([\d.]+)% rqAvg=(\d+)K rqMax=\d+K rqCap=\d+K resends=(\d+) selective=(\d+) writes=(\d+)',prov):
        P.setdefault(m.group(1),[]).append(tuple(float(x) for x in m.groups()[1:]))
    if not P: return None
    d=max(P,key=lambda k:sum(x[6] for x in P[k])); R=[x for x in P[d] if x[6]>3000]
    R=R[2:-2] if len(R)>6 else R
    med=lambda i: st.median([x[i] for x in R])
    B=[float(x) for x in re.findall(r'blocked=([\d.]+)%',cli)][2:-2]
    past=[int(x) for x in re.findall(r' past=(\d+)',cli)][2:-2]
    return med(0),med(4),med(6),(st.median(B) if B else -1),(st.median(past) if past else -1)
out=open('sackab.out').read()
rows={}
for m in re.finditer(r'(sk-(\w+)-r(\d)) bin=\S+ goodput=(\d+)',out):
    L,a,r,g=m.group(1),m.group(2),int(m.group(3)),int(m.group(4))
    s=stat(L); rows.setdefault(a,[]).append((r,g,s))
for a in ['ctl','S','R','SR','SR2']:
    v=rows.get(a,[])
    gs=[x[1] for x in v]
    print("%-4s goodput %-22s median %5.0f | noCap %s | resends/s %s writes/s %s | client blocked %s | past/s %s"%(a,gs,st.median(gs) if gs else 0,
      [round(x[2][0]) for x in v if x[2]],[round(x[2][1]) for x in v if x[2]],[round(x[2][2]) for x in v if x[2]],[round(x[2][3],1) for x in v if x[2]],[x[2][4] for x in v if x[2]]))
# paired by rotation vs ctl
c={x[0]:x[1] for x in rows.get('ctl',[])}
for a in ['S','R','SR','SR2']:
    print(a,"paired % vs ctl by rotation:",[round(100*(x[1]-c[x[0]])/c[x[0]]) for x in rows.get(a,[]) if x[0] in c])
