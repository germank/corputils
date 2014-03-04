def gziplines(fname):
    from subprocess import Popen, PIPE
    f = Popen(['zcat' ] + [fname], stdout=PIPE)
    for line in f.stdout:
        yield line