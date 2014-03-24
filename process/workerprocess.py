__author__ = 'Sagar'
from pygraphdb.services.common.communicationservice import Communication
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
        communication_service = Communication(host='localhost', port=4545, service_name='Worker', server=False)
        communication_service.startup()
        self._services['CommunicationService'] = communication_service
        communication_service.register('CommunicationService', communication_service.get_queue())

        heartbeat_service = HeartBeatService(communication_service=communication_service, interval=5)
        communication_service.register('HeartBeatService', heartbeat_service.get_queue())
        heartbeat_service.startup()
        self._services['HeartBeatService'] = heartbeat_service

        storage_service = StorageService(directory="C:\\Users\\Sagar\\PycharmProjects\\pygraphdb\\pygraphdb", communication_service=communication_service)
        communication_service.register('StorageService', storage_service.get_queue())
        storage_service.startup()
        self._services['StorageService'] = storage_service

    def stop(self):
        for service in self._services.values():
            service.shutdown()