__author__ = 'Sagar'

from pygraphdb.process.workerprocess import WorkerProcess
import logging
import time

logging.basicConfig(format='%(asctime)s\t%(levelname)-10s\t%(threadName)-32s\t%(name)-32s\t%(message)s', level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S')

worker = WorkerProcess()
worker.init('./worker.ini')
worker.run()

time.sleep(50)
worker.stop()