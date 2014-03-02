__author__ = 'Sagar'
from threading import Thread
import logging
import socket

from pygraphdb.services.common.socketwrapper import SocketReadWrite
from pygraphdb.services.common.handlers.receivehandler import ReceiveHandler
from pygraphdb.services.common.handlers.sendhandler import SendHandler
from pygraphdb.services.common.client import Client


class ConnectionHandler(Thread):
    def __init__(self, host, port, name, comm_service):
        super(ConnectionHandler, self).__init__()
        self._host = host
        self._port = port
        self._name = name
        self._communication_service = comm_service
        self._logger = logging.getLogger(self.__class__.__name__)
        self.socket = None
        self._running = True
        self._handlers = []
        self._clients = []

    def send(self, target_name, target_service, message):
        if target_name == self._communication_service.get_name():
            self._communication_service.get_queue().put(message)
        else:
            self._communication_service.get_client(target_name).get_send_handler().send(target_name, target_service, message)

    def run(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.settimeout(5)
        self.socket.bind((self._host, self._port))
        self.socket.listen(5)

        while self._running:
            try:
                (client_socket, address) = self.socket.accept()
            except socket.timeout:
                continue
            self._logger.info('New client request received from [%s:%d]', address[0], address[1])
            client_socket_wrapper = SocketReadWrite(client_socket)
            client = self.handshake(client_socket_wrapper)
            self._clients.append(client)
            self._communication_service.add_client(client)

            receive_handler = ReceiveHandler(self._communication_service, client_socket_wrapper, client)
            receive_handler.start()
            self._handlers.append(receive_handler)

            send_handler = SendHandler(self._communication_service, client)
            send_handler.start()
            self._handlers.append(send_handler)

            client.set_handlers(receive_handler, send_handler)

        for handler in self._handlers:
            handler.stop()
            handler.join()
        for client in self._clients:
            client.close()

        self._logger.info("Server Connection Handler [%s] run complete.", self._name)

    def handshake(self, client_socket_wrapper):
        client_info = client_socket_wrapper.read()
        client_socket_wrapper.write(self._name)
        client = Client(client_info, client_socket_wrapper)
        self._logger.info('Handshake complete with client [%s].', client.get_name())
        return client

    def stop(self):
        self._running = False
        self._logger.info("Server Connection Handler [%s] stop requested.", self._name)
