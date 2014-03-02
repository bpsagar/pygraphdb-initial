__author__ = 'Sagar'

from pygraphdb.process.workerprocess import WorkerProcess
import logging
import time

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)

worker = WorkerProcess()
worker.init('./worker.ini')
worker.run()

time.sleep(20)
worker.stop()