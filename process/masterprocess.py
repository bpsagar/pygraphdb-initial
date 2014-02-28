__author__ = 'Sagar'
from pygraphdb.services.common.communicationservice import CommunicationService
from pygraphdb.services.common.heartbeatservice import HeartBeatService
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
        heartbeat_service = HeartBeatService(communication_service, 5)
        communication_service.register('HeartBeatService', heartbeat_service.get_queue())
        heartbeat_service.start()
        self._services['HeartBeatService'] = heartbeat_service