import re,sys
t=open(sys.argv[1]).read()
rows=[]
for m in re.finditer(r'hold\[n=(\d+)[^]]*\].*?holes\[n<10=(\d+) n<50=(\d+) n<100=(\d+) n<300=(\d+) n<1000=(\d+) n>=1000=(\d+) ms<10=(\d+) ms<50=(\d+) ms<100=(\d+) ms<300=(\d+) ms<1000=(\d+) ms>=1000=(\d+)\]',t):
    v=list(map(int,m.groups()))
    if v[0]>3000: rows.append(v[1:])
rows=rows[3:-3]
n=len(rows); S=[sum(r[i] for r in rows) for i in range(12)]
print("steady s=%d  holes/s by duration <10:%.0f <50:%.1f <100:%.1f <300:%.2f <1000:%.2f >=1000:%.2f"%(n,*[S[i]/n for i in range(6)]))
tot=sum(S[6:])
print("  blocked share of wall time by class: <10:%.1f%% <50:%.1f%% <100:%.1f%% <300:%.1f%% <1000:%.1f%% >=1000:%.1f%%  total %.1f%%"%(*[100*S[6+i]/(n*1000) for i in range(6)],100*tot/(n*1000)))
