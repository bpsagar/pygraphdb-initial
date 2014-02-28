__author__ = 'Sagar'
from pygraphdb.process.masterprocess import MasterProcess
import logging

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)
master = MasterProcess()
master.init('./master.ini')
master.run()