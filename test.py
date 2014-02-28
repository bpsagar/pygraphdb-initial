__author__ = 'Sagar'

from pygraphdb.services.common.communicationservice import CommunicationService
import socket
import _thread
import logging
import time
from queue import Queue

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)

master = CommunicationService('localhost', 4545, 'Master', True)
worker = CommunicationService('localhost', 4545, 'Worker1', False)
worker1 = CommunicationService('localhost', 4545, 'Worker2', False)
q1 = Queue()
q2 = Queue()
master.register('Q1', q1)
worker.register('Q2', q2)
master.start()
worker.start()
worker1.start()
time.sleep(5)
master.send('Worker1', 'Q2', 'Hellooo!!')
worker1.send('Master', 'Q1', 'Heeeyyyy!!')
time.sleep(5)
print(q1.qsize())
print(q2.qsize())
worker.stop()
worker1.stop()
master.stop()
