__author__ = 'Sagar'
from pygraphdb.process.masterprocess import MasterProcess
import logging
import time

logging.basicConfig(format='%(asctime)s\t%(levelname)-10s\t%(threadName)-32s\t%(name)-32s\t%(message)s', level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S')
master = MasterProcess()
master.init('./master.ini')
master.run()

time.sleep(50)
master.stop()