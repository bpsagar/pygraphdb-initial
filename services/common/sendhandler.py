__author__ = 'Sagar'
from threading import Thread
import logging
from queue import Queue, Empty

class SendHandler(Thread):
    def __init__(self, comm_service, client):
        super(SendHandler, self).__init__()
        self._communication_service = comm_service
        self._logger = logging.getLogger(self.__class__.__name__)
        self._queue = Queue()
        self._client = client
        self._running = True

    def send(self, target_name, target_service, message):
        self._queue.put((target_name, target_service, message))

    def run(self):
        self._logger.info('Starting Send Handler for node [%s].', self._client.get_name())
        while self._running :
            try:
                (target_name, target_service, message) = self._queue.get(True, 5)
            except Empty:
                continue
            if target_name == self._communication_service.get_name():
                self._logger.info('Sending message [%s] to target service [%s].', str(message), target_service)
                self._communication_service.get_service_queue(target_service).put(message)
            else:
                self._logger.info('Sending message [%s] to target service [%s] on node [%s].', str(message), target_service, target_name)
                self._communication_service.get_client(target_name).send_message(target_service, message)
        self._logger.info("Send Handler run complete.")

    def stop(self):
        self._running = False
        self._logger.info("Send Handler stop requested.")