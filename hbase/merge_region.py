#! /usr/bin/env python
# For a hbase table, merge every pair of consecutive regions together
# need to run on the hbase shell machine
import os,sys,time
import re
from subprocess import Popen, PIPE, STDOUT

if len(sys.argv) < 2:
    print "usage merge.py <tablename>"
    sys.exit(1)

def regionid(line):
    m = re.search(r'ENCODED\s\=\>\s(\w*),', line)
    if m:
        return m.group(1)

def hbexec(cmd):
    print cmd
    p = Popen(["hbase", "shell"], stdout=PIPE, stdin=PIPE, stderr=STDOUT)
    return p.communicate(input=cmd)[0]

rs = hbexec("scan 'hbase:meta',{FILTER=>\"PrefixFilter('%s')\", COLUMNS => 'info:regioninfo'}" % sys.argv[1])
rs = map(regionid, rs.split("\n"))
rs = filter(lambda x:x, rs)
for i in range((len(rs) - 1)/2) :
    hbexec("merge_region '%s', '%s'" % (rs[i*2],rs[i*2+1]))
    print "sleep 20 seconds"
    time.sleep(20)
