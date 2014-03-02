__author__ = 'Sagar'
from pygraphdb.services.common.service import Service
from pygraphdb.services.messages.addnode import AddNode
from pygraphdb.services.messages.deletenode import DeleteNode
from pygraphdb.services.messages.updatenode import UpdateNode
from queue import Queue, Empty
import json
import logging
import os
from pygraphdb.services.worker.node import Node

class StorageService(Service):
    def __init__(self, dir, comm_service):
        super(StorageService, self).__init__()
        self._directory = dir
        self._communication_service = comm_service
        self._node_file = "nodedata"
        self._edge_file = "edgedata"
        self._queue = Queue()
        self._running = True
        self._logger = logging.getLogger(self.__class__.__name__)

    def run(self):
        self._logger.info("Storage Service started on node [%s].", self._communication_service.get_name())
        node_file_path = os.path.join(self._directory, self._node_file)
        self._node_fd = open(node_file_path, "rb+")
        edge_file_path = os.path.join(self._directory, self._node_file)
        self._edge_fd = open(edge_file_path, "rb+")
        while self._running:
            try:
                message = self._queue.get(True, 5)
            except Empty:
                continue
            if isinstance(message, AddNode):
                self.add_node(message.get_node())

            if isinstance(message, DeleteNode):
                self.delete_node(message.get_node().get_id(), message.get_index())

            if isinstance(message, UpdateNode):
                self.update_node(message.get_node(), message.get_index())
        self._node_fd.close()
        self._edge_fd.close()

    def add_node(self, node):
        data = node.dump()
        data_size = "%08x" % len(data)
        self._node_fd.seek(0, 2) # 2 - end of file reference
        record = data_size + " " + data
        self._node_fd.write(bytes(record, 'UTF-8'))
        self._logger.info("Added a new node [id=%s] to the database.", node.get_id())

    def read_node(self):
        delete_marker = '*'
        node = None
        size = 0
        while delete_marker == '*':
            size = self._node_fd.read(8).decode("UTF-8")
            if size == '':
                return (None, None)
            node_position = self._node_fd.tell()
            delete_marker = self._node_fd.read(1).decode("UTF-8")
            size = int(size, 16)
            if delete_marker == '*':
                self._node_fd.seek(size, 1)
        full_data = ''
        while size > 0:
            data = self._node_fd.read(size).decode("UTF-8")
            size -= len(data)
            full_data += data
        node = Node()
        node.load(full_data)
        return (node, node_position)

    def delete_node(self, node_id, index=None):
        if index is None:
            self._node_fd.seek(0, 0) # 0 - beginning of the file reference
            node = None
            while True:
                node, node_position = self.read_node()
                if node is None:
                    break
                if node.get_id() == node_id:
                    break
        else:
            node_position = index
            self._node_fd.seek(index,0)
            node = self.return_node()

        if node:
            self._node_fd.seek(node_position, 0)
            self._node_fd.write(bytes('*', "UTF-8"))
            self._logger.info("Deleted node [id=%s].", node_id)
        else:
            self._logger.warning("Could not find node [id=%s].", node_id)

    def update_node(self, node, index=None):
        self.delete_node(node.get_id(), index)
        self.add_node(node)
        self._logger.info("Updated node [id=%s].", node.get_id())

    def stop(self):
        self._running = False
        self._logger.info("Storage Service stopped on node [%s].", self._communication_service.get_name())

    def get_queue(self):
        return self._queue