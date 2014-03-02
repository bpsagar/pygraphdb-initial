__author__ = 'Sagar'
from pygraphdb.process.masterprocess import MasterProcess
import logging
import time

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)
master = MasterProcess()
master.init('./master.ini')
master.run()

time.sleep(20)
master.stop()