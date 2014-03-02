__author__ = 'Sagar'
from pygraphdb.services.common.communicationservice import CommunicationService
from pygraphdb.services.common.heartbeatservice import HeartBeatService
from pygraphdb.services.worker.node import Node
from pygraphdb.services.messages.addnode import AddNode
from pygraphdb.services.messages.updatenode import UpdateNode
from pygraphdb.services.messages.deletenode import DeleteNode
from pygraphdb.services.worker.edge import Edge
from pygraphdb.services.messages.addedge import AddEdge
from pygraphdb.services.messages.deleteedge import DeleteEdge
from pygraphdb.services.messages.updateedge import UpdateEdge
import time
import configparser

class MasterProcess(object):
    def __init__(self,):
        super(MasterProcess, self).__init__()
        self._name = None
        self._port = None
        self._hostname = None
        self._services = {}

    def init(self, config_path):
        config = configparser.ConfigParser(allow_no_value=True)
        config.read(config_path)
        self._name = config['Node']['Name']
        self._port = int(config['Node']['Port'])
        self._hostname = config['Node']['Hostname']

    def run(self):
        communication_service = CommunicationService(self._hostname, self._port, self._name, True)
        communication_service.start()
        self._services['CommunicationService'] = communication_service
        communication_service.register('CommunicationService', communication_service.get_queue())

        heartbeat_service = HeartBeatService(communication_service, 5)
        communication_service.register('HeartBeatService', heartbeat_service.get_queue())
        heartbeat_service.start()
        self._services['HeartBeatService'] = heartbeat_service

        self.test_storage()

    def stop(self):
        for service in self._services.values():
            service.stop()

    def test_storage(self):
        node = Node()
        node._id = 1
        node._type = 'Person'
        node.set_property('Name', 'Sagar')
        node.set_property('Age', 21)
        time.sleep(3)
        add_node = AddNode(node)
        self._services['CommunicationService'].send('WORKER1', 'StorageService', add_node)

        edge = Edge()
        edge._id = 10
        edge._type = 'Relation'
        edge._node1_id = 1
        edge._node2_id = 2
        edge.set_property('Name', 'Sagar')
        edge.set_property('Age', 21)
        time.sleep(3)
        update_edge = UpdateEdge(edge)
        self._services['CommunicationService'].send('WORKER1', 'StorageService', update_edge)