__author__ = 'Sagar'
from threading import Thread
import logging
from pickle import loads
import socket
import time

class ReceiveHandler(Thread):
    def __init__(self, comm_service, socket_wrapper, client):
        super(ReceiveHandler, self).__init__()
        self._communication_service = comm_service
        self._socket_wrapper = socket_wrapper
        self._logger = logging.getLogger(self.__class__.__name__)
        self._client = client
        self._running = True

    def run(self):
        self._logger.info('Starting Receive Handler for node [%s].', self._client.get_name())
        self._socket_wrapper.set_timeout(5)
        while self._running:
            try:
                data = loads(self._socket_wrapper.read_bytes())
            except socket.timeout:
                continue
            except EOFError:
                self._logger.warning("EOF occurred while reading incoming message.")
                time.sleep(1)
                continue
            self._logger.debug('Received message [%s] for the service [%s].', str(data['message']), data['target_service'])
            self._communication_service.get_service_queue(data['target_service']).put(data['message'])
        self._logger.info("Receive Handler run complete.")

    def stop(self):
        self._running = False
        self._logger.info("Receive Handler stop requested.")
