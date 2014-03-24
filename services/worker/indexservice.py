__author__ = 'Sagar'
from pygraphdb.services.common.service import Service
from pygraphdb.services.messages.addindex import AddIndex
from pygraphdb.services.messages.deleteindex import DeleteIndex
from pygraphdb.services.messages.findindex import FindIndex
from pygraphdb.services.worker.propertyindex import PropertyIndex
from queue import Queue, Empty
from struct import pack, unpack

import os

class IndexService(Service):
    def init(self):
        pass

    def construct(self, config):
        self._block_size = config.get('block_size', 1024)
        self._queue = Queue()
        self._timeout = 1
        self._index_directory = config.get('index_directory')
        self._indexing_objects = {}

    def deinit(self):
        for obj in self._indexing_objects.values():
            obj.stop()

    def do_work(self):
        try:
            message = self._queue.get(True, self._timeout)
        except Empty:
            raise TimeoutError

        if isinstance(message, AddIndex):
            index_name = message.get_index_name()
            if index_name not in self._indexing_objects.keys():
                property_object = PropertyIndex(index_name, self._index_directory, self._block_size)
                self._indexing_objects[index_name] = property_object
            self._indexing_objects[index_name].add_index(message.get_index_node(), None)

        if isinstance(message, FindIndex):
            index_name = message.get_index_name()
            if index_name not in self._indexing_objects.keys():
                property_object = PropertyIndex(index_name, self._index_directory, self._block_size)
                self._indexing_objects[index_name] = property_object
            self._indexing_objects[index_name].find_index_node(message.get_key())

        if isinstance(message, DeleteIndex):
            index_name = message.get_index_name()
            if index_name not in self._indexing_objects.keys():
                property_object = PropertyIndex(index_name, self._index_directory, self._block_size)
                self._indexing_objects[index_name] = property_object
            self._indexing_objects[index_name].delete_index_node(message.get_index_node())
        return True