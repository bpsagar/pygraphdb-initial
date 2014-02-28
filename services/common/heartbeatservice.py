__author__ = 'Sagar'
from pygraphdb.services.common.service import Service
from pygraphdb.services.messages.heartbeat import HeartBeat
from pygraphdb.services.messages.heartbeatresponse import HeartBeatResponse
from pygraphdb.services.messages.deadnode import DeadNode
from queue import Queue, Empty
import datetime
import time
import logging

class HeartBeatService(Service):
    def __init__(self, comm_service, interval):
        super(HeartBeatService, self).__init__()
        self._queue = Queue()
        self._communication_service = comm_service
        self._clients = []
        self._running = True
        self._interval = interval
        self._clients_counter = {}
        self._logger = logging.getLogger(self.__class__.__name__)

    def run(self):
        self._logger.info("HeartBeat Service registered for node [%s].", self._communication_service.get_name())
        self._logger.info("HeartBeat Service for node [%s] started.", self._communication_service.get_name())
        while self._running:
            self._clients[:] = [client for client in self._communication_service.get_client_list()]
            time.sleep(self._interval)
            self.process_queue()
            for client in self._clients:

                if client.get_name() not in self._clients_counter.keys():
                    self._clients_counter[client.get_name()] = 0
                elif self._clients_counter[client.get_name()] < 5:
                    self.send_heartbeat(client)
                else:
                    self._logger.info("Dead node [%s].", client.get_name())
                    message = DeadNode(client)
                    self._clients_counter.pop(client.get_name())
                    self._communication_service.get_queue().put(message)

    def process_queue(self):
        queue_size = self._queue.qsize()
        while queue_size != 0:
            message = self._queue.get()
            if isinstance(message, HeartBeat):
                self._logger.info("Received HeartBeat message from node [%s].", message.get_sender())
                self.send_reply_heartbeat(message)
            elif isinstance(message, HeartBeatResponse):
                counter = message.get_counter()
                sender = message.get_sender()
                self._logger.info("Received HeartBeatResponse message from node [%s].", message.get_sender())
                if counter == self._clients_counter[sender]:
                    self._logger.info("Resetting HeartBeat counter for node [%s].", message.get_sender())
                    self._clients_counter[sender] = 0

            queue_size -= 1

    def send_heartbeat(self, client):
        target_name = client.get_name()
        target_service = 'HeartBeatService'
        counter = self._clients_counter[target_name]
        heart_beat = HeartBeat(self._communication_service.get_name(), str(datetime.datetime.utcnow()), counter+1)
        self._clients_counter[target_name] += 1
        self._communication_service.send(target_name, target_service, heart_beat)
        self._logger.info("Sent HeartBeat message to node [%s].", target_name)

    def send_reply_heartbeat(self, message):
        target_name = message.get_sender()
        counter = message.get_counter()
        target_service = 'HeartBeatService'
        heartbeat_response = HeartBeatResponse(self._communication_service.get_name(), str(datetime.datetime.utcnow()), counter)
        self._communication_service.send(target_name, target_service, heartbeat_response)
        self._logger.info("Sent HeartBeatResponse message to node [%s].", target_name)

    def get_queue(self):
        return self._queue

    def stop(self):
        self._running = False