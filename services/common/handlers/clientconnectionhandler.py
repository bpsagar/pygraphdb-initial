__author__ = 'Sagar'
from threading import Thread
import socket
import logging
import time

from pygraphdb.services.common.socketwrapper import SocketReadWrite
from pygraphdb.services.common.handlers.receivehandler import ReceiveHandler
from pygraphdb.services.common.handlers.sendhandler import SendHandler
from pygraphdb.services.common.client import Client


class ClientConnectionHandler(Thread):
    def __init__(self, host, port, name, comm_service):
        super(ClientConnectionHandler, self).__init__()
        self._host = host
        self._port = port
        self._name = name
        self._communication_service = comm_service
        self._logger = logging.getLogger(self.__class__.__name__)
        self._socket_wrapper = None
        self._send_handler = None
        self._receive_handler = None
        self._master_node_name = None
        self._running = True

    def run(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket_wrapper = SocketReadWrite(s)
        self._logger.info('Connecting to master node.')
        s.connect((self._host, self._port))
        client = self.handshake(self._socket_wrapper)
        self._communication_service.add_client(client)
        self._logger.info('Connection to master node successful.')
        self._receive_handler = ReceiveHandler(self._communication_service, self._socket_wrapper, client)
        self._receive_handler.start()
        self._send_handler = SendHandler(self._communication_service, client)
        self._send_handler.start()

        while self._running:
            time.sleep(5)

        self._send_handler.stop()
        self._send_handler.join()
        self._receive_handler.stop()
        self._receive_handler.join()
        self._socket_wrapper.close()
        self._logger.info("Client Connection Handler [%s] run complete.", self._name)

    def send(self, target_name, target_service, message):
        self._send_handler.send(target_name, target_service, message)

    def handshake(self, socket_wrapper):
        socket_wrapper.write(self._name)
        self._master_node_name = socket_wrapper.read()
        client = Client(self._master_node_name, socket_wrapper)
        self._logger.info('Handshake complete with the master node [%s]', self._master_node_name)
        return client

    def stop(self):
        self._running = False
        self._logger.info("Client Connection Handler [%s] stop requested.", self._name)