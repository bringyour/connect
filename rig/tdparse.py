import sys,re,collections,statistics as st
runs=[];last=None
for l in open(sys.argv[1]):
    m=re.search(r'I\d{4} (\d+):(\d+):([\d.]+).*dest=(\S+) loops=(\d+) noCap=([\d.]+)% otherNil=([\d.]+)% starve=([\d.]+)% work=([-\d.]+)% rqAvg=(\d+)K rqMax=(\d+)K rqCap=(\d+)K(?: resends=(\d+) selective=(\d+))?',l)
    if not m: continue
    t=int(m.group(1))*3600+int(m.group(2))*60+float(m.group(3))
    if last is None or t-last>20: runs.append(collections.defaultdict(list))
    last=t
    runs[-1][m.group(4)].append([float(x) if x is not None else -1 for x in m.groups()[4:]])
for i,rows in enumerate(runs):
    d=max(rows,key=lambda k:len(rows[k])); r=rows[d][2:-2] or rows[d]
    med=lambda j: st.median(x[j] for x in r)
    print(f"run{i+1} dest={d[:8]} n={len(r)}s loops/s={med(0):.0f} noCap={med(1):.1f}% otherNil={med(2):.1f}% starve={med(3):.1f}% work={med(4):.1f}% rqAvg={med(5):.0f}K rqMax={med(6):.0f}K cap={med(7):.0f}K resends/s={med(8):.0f} selective/s={med(9):.0f} others={len(rows)-1}")
