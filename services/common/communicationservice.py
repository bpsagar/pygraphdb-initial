__author__ = 'Sagar'
import logging
from queue import Queue, Empty

from pygraphdb.services.common.service import Service
from pygraphdb.services.common.handlers.connectionhandler import ConnectionHandler
from pygraphdb.services.common.handlers.clientconnectionhandler import ClientConnectionHandler
from pygraphdb.services.common.heartbeatservice import DeadNode


class CommunicationService(Service):

    def __init__(self, host, port, name, server=False):
        super(CommunicationService, self).__init__()
        self._server = server
        self._host = host
        self._port = port
        self._service_queues = {}
        self._name = name
        self._logger = logging.getLogger(self.__class__.__name__)
        self._clients = {}
        self._handler = None
        self._queue = Queue()
        self._running = True

    def run(self):
        self._logger.info('Starting Communication Service for node [%s].', self._name)

        if self._server:
            connection_handler = ConnectionHandler(self._host, self._port, self._name, self)
            connection_handler.start()
            self._handler = connection_handler
        else:
            client_connection_handler = ClientConnectionHandler(self._host, self._port, self._name, self)
            client_connection_handler.start()
            self._handler = client_connection_handler

        while self._running:
            try:
                message = self._queue.get(True, 5)
            except Empty:
                continue

            if isinstance(message, DeadNode):
                if self._server:
                    self.remove_client(message.get_client())
                    self._logger.info("Removed client [%s] from client list.", message.get_client().get_name())
                else:
                    self.remove_client(message.get_client())
                    self._logger.info("Removed client [%s] from client list.", message.get_client().get_name())
                    pass

        self._handler.join()
        self._logger.info("Communication Service for node [%s] complete.", self._name)

    def add_client(self, client):
        self._clients[client.get_name()] = client
        self._logger.info('New client [%s] connected.', client.get_name())

    def send(self, target_name, target_service, message):
        self._handler.send(target_name, target_service, message)

    def get_name(self):
        return self._name

    def get_client(self, client_name):
        return self._clients[client_name]

    def register(self, service_name, q):
        self._service_queues[service_name] = q

    def get_service_queue(self, service_name):
        return self._service_queues.get(service_name, None)

    def stop(self):
        self._handler.stop()

    def get_client_list(self):
        return self._clients.values()

    def remove_client(self, client):
        if client.get_name() in self._clients.keys():
            self._clients.pop(client.get_name())

    def get_queue(self):
        return self._queue
