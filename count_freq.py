#!/usr/bin/env python
import sys

progress = 0
freqs = {}
for l in sys.stdin:
    if l not in freqs:
        freqs[l] = 0
    freqs[l] += 1
    progress += 1
    if progress % 100000 == 0:
        sys.stderr.write('.')
    if progress % 10000000 == 0:
        sys.stderr.write('\n')
        sys.stderr.flush()

for w,f in sorted(freqs.iteritems(), key=lambda (k,v): -v):
    print "{0}\t{1}".format(w.rstrip('\n'),f)
