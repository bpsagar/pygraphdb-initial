__author__ = 'Sagar'
from pygraphdb.services.common.communicationservice import CommunicationService
from pygraphdb.services.common.heartbeatservice import HeartBeatService
from pygraphdb.services.worker.storageservice import StorageService
import configparser

class WorkerProcess(object):
    def __init__(self):
        super(WorkerProcess, self).__init__()
        self._name = None
        self._master_port = None
        self._master_hostname = None
        self._services = {}

    def init(self, config_path):
        config = configparser.ConfigParser(allow_no_value=True)
        config.read(config_path)
        self._name = config['Node']['Name']
        self._master_port = int(config['Node']['MasterNodePort'])
        self._master_hostname = config['Node']['MasterNodeHostname']

    def run(self):
        communication_service = CommunicationService(self._master_hostname, self._master_port, self._name, False)
        communication_service.start()
        self._services['CommunicationService'] = communication_service
        communication_service.register('CommunicationService', communication_service.get_queue())

        heartbeat_service = HeartBeatService(communication_service, 5)
        communication_service.register('HeartBeatService', heartbeat_service.get_queue())
        heartbeat_service.start()
        self._services['HeartBeatService'] = heartbeat_service

        storage_service = StorageService("C:\\Users\\Sagar\\PycharmProjects\\pygraphdb\\pygraphdb", communication_service)
        communication_service.register('StorageService', storage_service.get_queue())
        storage_service.start()
        self._services['StorageService'] = storage_service

    def stop(self):
        for service in self._services.values():
            service.stop()