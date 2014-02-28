__author__ = 'Sagar'

from communicationservice import CommunicationService
import socket

worker = CommunicationService('localhost', 4545, False)
worker.start()