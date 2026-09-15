import re,sys
prov=open(sys.argv[1]).read(); cli=open(sys.argv[2]).read()
P={}
for m in re.finditer(r'TDIAG dest=(\S+) .*?resends=(\d+) selective=\d+ writes=(\d+) writeErrs=(\d+)',prov):
    P.setdefault(m.group(1),[]).append((int(m.group(3)),int(m.group(2)),int(m.group(4))))
d=max(P,key=lambda k:sum(x[0] for x in P[k])); P=P[d]
X=[tuple(map(int,m.groups())) for m in re.finditer(r'readsLarge=(\d+) handoffDrops=(\d+) unwrapErrs=(\d+) rqDrops=(\d+) notReceived=(\d+) nilSeq=(\d+) runIn=(\d+) runInLarge=(\d+) packReach=(\d+) packOk=(\d+) contractMissing=(\d+)',cli)]
C=[sum(map(int,m.groups())) for m in re.finditer(r'RDIAG src=\S+ head=(\d+) past=(\d+) fut1=(\d+) fut2_8=(\d+) fut9_64=(\d+) futBig=(\d+)',cli)]
S=lambda i:sum(x[i] for x in X)
w=sum(x[0] for x in P); res=sum(x[1] for x in P); we=sum(x[2] for x in P)
print("provider seq writes %d (resends %d, writeErrs %d)"%(w,res,we))
print("client ws readsLarge %d | runIn %d runInLarge %d | packReach %d packOk %d handoffDrops %d unwrapErrs %d | RDIAG arrivals %d contractMissing %d notReceived %d rqDrops %d nilSeq %d"%(S(0),S(6),S(7),S(8),S(9),S(1),S(2),sum(C),S(10),S(4),S(3),S(5)))
print("gaps: prov->runInLarge %+d  runInLarge->packReach %+d  packReach->packOk %+d  packOk->(RDIAG+contractMissing) %+d"%(w-S(7),S(7)-S(8),S(8)-S(9),S(9)-sum(C)-S(10)))
