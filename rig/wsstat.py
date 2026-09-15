import re,statistics as st,os
out=open('winsg.out').read()
rows={}
for m in re.finditer(r'(ws(\d)-(\w+)-r(\d)) bin=\S+ goodput=(\d+).*?stall=(\d+)',out):
    L,f,a,r,g,sl=m.group(1),m.group(2),m.group(3),int(m.group(4)),int(m.group(5)),int(m.group(6))
    prov=open(f'td-{L}.txt').read() if os.path.exists(f'td-{L}.txt') else ''
    R=[tuple(float(x) for x in mm.groups()) for mm in re.finditer(r'noCap=([\d.]+)%.*?rqAvg=(\d+)K.*?resends=(\d+) selective=\d+ writes=(\d+)',prov)]
    R=[x for x in R if x[3]>3000][2:-2]
    cli=open(f'cl-{L}.txt').read() if os.path.exists(f'cl-{L}.txt') else ''
    B=[float(x) for x in re.findall(r'blocked=([\d.]+)%',cli)][2:-2]
    past=[int(x) for x in re.findall(r' past=(\d+)',cli)][2:-2]
    med=lambda i: round(st.median([x[i] for x in R])) if R else -1
    rows.setdefault((f,a),[]).append((r,g,sl,med(0),med(1),med(2),round(st.median(B),1) if B else -1,st.median(past) if past else -1))
for k in sorted(rows):
    v=sorted(rows[k]); gs=[x[1] for x in v]
    print(f"f{k[0]} {k[1]:6s} goodput {gs} median {st.median(gs):.0f} stalls {sum(x[2] for x in v)} | noCap {[x[3] for x in v]} inflightK {[x[4] for x in v]} resends/s {[x[5] for x in v]} blocked {[x[6] for x in v]} dup/s {[x[7] for x in v]}")
