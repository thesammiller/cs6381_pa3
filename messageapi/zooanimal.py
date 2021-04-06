#
# Contents:
#    - ZooAnimal
#    - ZooLoad
#    - ZooProxy
#    - ZooClient

import codecs
import json

from kazoo.client import KazooClient, KazooState

from .util import local_ip4_addr_list

ZOOKEEPER_ADDRESS = "10.0.0.1"
ZOOKEEPER_PORT = "2181"
ZOOKEEPER_LOCATION = '{zookeeper_ip}:{zookeeper_port}'.format(zookeeper_ip=ZOOKEEPER_ADDRESS,
                                                              zookeeper_port=ZOOKEEPER_PORT)

ZOOKEEPER_PATH_STRING = "/{role}/{topic}"

# For mininet -> 10.0.0.x
NETWORK_PREFIX = "10.0.0"


class ZooAnimal:
    def __init__(self):
        self.zk = KazooClient(hosts=ZOOKEEPER_LOCATION)
        self.zk.start()
        # Use util function to get IP address
        self.ipaddress = [ip for ip in list(
            local_ip4_addr_list()) if ip.startswith(NETWORK_PREFIX)][0]
        # Inheriting children should assign values to fit the scheme
        self.role = None
        self.topic = None
        # for pub and sub
        self.broker = None
        # Zookeeper
        self.election = self.zk.Election('/broker', self.ipaddress)
        self.zk_seq_id = None
        self.zk_is_a_master = False

    def zookeeper_watcher(self, watch_path):
        @self.zk.DataWatch(watch_path)
        def zookeeper_election(data, stat, event):
            print("ZOOANIMAL -> Watching node -> ", data)
            if data is None:
                print("ZOOANIMAL -> Data is none.")
                self.election.run(self.zookeeper_register)

    def zookeeper_master(self):
        if not self.zk_is_a_master:
            print("ZOOANIMAL -> Becoming a master.")
            role_topic = "/broker/master"
            data = {'ip': self.ipaddress}
            data_string = json.dumps(data)
            encoded_ip = codecs.encode(data_string, "utf-8")
            self.zk.create(role_topic, ephemeral=True,
                           makepath=True, sequence=True, value=encoded_ip)
            self.zk_is_a_master = True
        return self.zk_is_a_master

    def zookeeper_register(self):
        pass

    def broker_update(self, data):
        pass

########################################
# LOAD
#

class ZooLoad(ZooAnimal):
    def __init__(self):
        super().__init__()
        self.role = "load"
        self.topic = "balance"
        self.zookeeper_register()

    def zookeeper_register(self):
        role_topic = ZOOKEEPER_PATH_STRING.format(
            role=self.role, topic=self.topic)
        encoded_ip = codecs.encode(self.ipaddress, "utf-8")
        try:
            self.zk.create(role_topic, ephemeral=True,
                           sequence=True, makepath=True, value=encoded_ip)
        except:
            print(
                "Exception -> zooanimal.py -> zookeeper_register -> load elif statement")

#############################
# PROXY
#

class ZooProxy(ZooAnimal):
    def __init__(self):
        super().__init__()
        self.role = "broker"
        self.topic = "pool"
        self.zk_seq_id = None
        self.zookeeper_register()

    def zookeeper_register(self):
        role_topic = "/broker/pool"
        data = {'ip' : self.ipaddress}
        data_string = json.dumps(data)
        encoded_ip = codecs.encode(data_string, "utf-8")
        broker_path = "/broker"
        if self.zk_seq_id is None:
            self.zk.create(role_topic, ephemeral=True,
                           sequence=True, makepath=True, value=encoded_ip)
            brokers = self.zk.get_children(broker_path)
            brokers = [x for x in brokers if "lock" not in x]
            brokers = [x for x in brokers if "master" not in x]
            broker_sort = sorted(brokers, key=lambda data: int(data[-5:]))
            latest_id = broker_sort[-1]
            self.zk_seq_id = latest_id
            previous = broker_sort[broker_sort.index(self.zk_seq_id) - 1]
            watch_path = broker_path + "/" + previous
            self.zookeeper_watcher(watch_path)

#######################
# ZooClient (Pub/Sub)
#

class ZooClient(ZooAnimal):
    def __init__(self, role=None, topic=None, history=None):
        super().__init__()
        self.role = role
        self.topic = topic
        self.history = history
        self.zookeeper_register()
        self.ownership = self.get_ownership()
        self.zk_seq_id = None

    def zookeeper_register(self):
        topic = "/topic/" + self.topic
        topic_role = topic + "/" + self.role
        json_data = {'ip': self.ipaddress, 'history': self.history}
        json_string = json.dumps(json_data)
        json_encoded = codecs.encode(json_string, 'utf-8')
        self.zk.create(topic_role, ephemeral=True, makepath=True,
                       sequence=True, value=json_encoded)
        topic_clients = self.zk.get_children(topic)
        topic_sort = sorted(topic_clients, key=lambda data: int(data[-5:]))
        self.zk_seq_id = topic_sort[-1]
        print("ZOOANIMAL ID -> {}".format(self.zk_seq_id))

    def get_ownership(self):
        topic = "/topic/" + self.topic
        topic_clients = self.zk.get_children(topic)
        topic_roles = [x for x in topic_clients if self.role in x]
        topic_sort = sorted(topic_roles, key=lambda data: int(data[-5:]))
        return topic_sort[0]
