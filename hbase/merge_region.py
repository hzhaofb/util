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

def merge(r1, r2):
    if r1 and r2:
        hbexec("merge_region '%s', '%s'" % (r1, r2))
        # wait for some time for hbase to catch up
        print "sleep 20"
        time.sleep(20)

rs = hbexec("scan 'hbase:meta',{FILTER=>\"PrefixFilter('%s')\", COLUMNS => 'info:regioninfo'}" % sys.argv[1])
# extract regionid
rs = map(regionid, rs.split("\n"))
rs = filter(lambda x:x, rs)
# group every 2 region ids and merge them
map(merge, *([iter(rs)] *2))
