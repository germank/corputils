import subprocess 
import time
import signal
import os

class DieAfterDone(object):
    '''We don't tolerate useless processes here'''
    def __init__(self, cmd, disable=False):
        #For debugging purposes
        self.disable = disable
        self.cmd = cmd
        self.proc = None
    def __enter__(self):
        if self.disable:
            return
        print "Running {0}".format(" ".join(self.cmd))
        self.proc = subprocess.Popen(self.cmd, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, stdin=subprocess.PIPE,
            preexec_fn=os.setsid)
        print self.proc.pid
        time.sleep(1)
        self.proc.poll()
        assert not self.proc.returncode, "Could not start {0}"\
            "({1}): {2}".format(self.cmd, self.proc.returncode,
            self.proc.stdout.read())
    def __exit__(self, *args):
        if self.disable:
            return
        print "KILLING process"
        os.kill(self.proc.pid, signal.SIGHUP)
