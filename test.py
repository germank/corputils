import subprocess
from kyototycoon import *
from utils import *
import time

with DieAfterDone(['ktserver']):
    time.sleep(1)
    print 'starting'
    db = KyotoTycoon()
    print 'setting'
    db.set('a', '1', 1)
    print 'closing'
    db.close()
    print 'done'
