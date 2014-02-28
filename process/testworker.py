__author__ = 'Sagar'

from pygraphdb.process.workerprocess import WorkerProcess
import logging

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)

worker = WorkerProcess()
worker.init('./worker.ini')
worker.run()