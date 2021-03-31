#
# Team 6
# Programming Assignment #2
#
# Contents:
#    - ZooAnimal
#

import codecs
import time
import threading

from kazoo.client import KazooClient, KazooState

from .util import local_ip4_addr_list

ZOOKEEPER_ADDRESS = "10.0.0.1"
ZOOKEEPER_PORT = "2181"
ZOOKEEPER_LOCATION = '{zookeeper_ip}:{zookeeper_port}'.format(zookeeper_ip=ZOOKEEPER_ADDRESS,
                                                              zookeeper_port=ZOOKEEPER_PORT)
# For mininet -> 10.0.0.x
NETWORK_PREFIX = "10"


ZOOKEEPER_PATH_STRING = '/{role}/{topic}'




#####################################################
#
# ZooAnimal for Zookeeper Registrants
# Broker will Overload Zookeeper Register
# Properties must be defined by children:
#   - role
#   - approach
#   - topic
#
######################################################


class ZooAnimal:
    def __init__(self):
        self.zk = KazooClient(hosts=ZOOKEEPER_LOCATION)
        self.zk.start()

        # Use util function to get IP address
        self.ipaddress = [ip for ip in list(local_ip4_addr_list()) if ip.startswith(NETWORK_PREFIX)][0]

        # Inheriting children should assign values to fit the scheme
        # /role/topic
        self.role = None
        self.topic = None
        #Will only be set by pub and sub
        self.broker = None
        # Zookeeper
        #self.election = None
        self.election = self.zk.Election('/broker', self.ipaddress)
        self.zk_seq_id = None

    def zookeeper_watcher(self, watch_path):
        @self.zk.DataWatch(watch_path)
        def zookeeper_election(data, stat, event):
            print("Setting election watch.")
            print("Watching node -> ", data)
            if data is None:
                print("Data is none.")
                self.election.run(self.zookeeper_register)
                #self.election.cancel()

    def zookeeper_master(self):
        print("Becoming a master.")
        role_topic = ZOOKEEPER_PATH_STRING.format(role=self.role, topic='master')
        encoded_ip = codecs.encode(self.ipaddress, "utf-8")
        self.zk.create(role_topic, ephemeral=True, makepath=True, sequence=True, value=encoded_ip)
        return True

    def zookeeper_register(self):
        pass

    # This is a function stub for the get_broker watch callback
    # The child is expected to implement their own logic
    # Pub and Sub need to register_sub()
    def broker_update(self, data):
        print("Broker updated.")
        print("Data -> {}".format(data))
        pass


##################################################################################################

########################################
# Load
#
################################

class ZooLoad(ZooAnimal):
    def __init__(self):
        super().__init__()

    def zookeeper_register(self):
        role_topic = ZOOKEEPER_PATH_STRING.format(role=self.role, topic=self.topic)
        encoded_ip = codecs.encode(self.ipaddress, "utf-8")
        try:
            self.zk.create(role_topic, ephemeral=True, sequence=True, makepath=True, value=encoded_ip)
        except:
            print("Exception -> zooanimal.py -> zookeeper_register -> load elif statement")


#############################
# PROXY
#
#############################

class ZooProxy(ZooAnimal):
    def __init__(self):
        super().__init__()
        self.zk_seq_id = None

    def zookeeper_register(self):
        role_topic = ZOOKEEPER_PATH_STRING.format(role=self.role, topic=self.topic)
        print("ZooProxy Register -> {}".format(self.ipaddress))
        encoded_ip = codecs.encode(self.ipaddress, "utf-8")
        broker_path = "/broker"
        if self.zk_seq_id is None:
            print("Creating Pool Sequential ID Node")
            self.zk.create(role_topic, ephemeral=True, sequence=True, makepath=True, value=encoded_ip)
            print("Getting children of brokers")
            brokers = self.zk.get_children(broker_path)
            brokers = [x for x in brokers if "lock" not in x]
            brokers = [x for x in brokers if "master" not in x]
            print(brokers)
            broker_nums = {y: int(y[4:]) for y in brokers}
            # sort based on the values
            broker_sort = sorted(broker_nums, key=lambda data: broker_nums[data])
            latest_id = broker_sort[-1]
            print(latest_id)
            self.zk_seq_id = latest_id
            previous = broker_sort[broker_sort.index(self.zk_seq_id) - 1]
            # previous = path_sort[-1]
            watch_path = broker_path + "/" + previous
            self.zookeeper_watcher(watch_path)


#######################
# ZooClient (Pub/Sub)
#
#######################


class ZooClient(ZooAnimal):
    def __init__(self):
        super().__init__()

    def zookeeper_register(self):
        role_topic = ZOOKEEPER_PATH_STRING.format(role=self.role, topic=self.topic)
        # zk.ensure_path checks if path exists, and if not it creates it
        try:
            self.zk.create(role_topic, ephemeral=True, makepath=True)
        except:
            print("Topic already exists.")

        # get the string from the path - if it's just created, it will be empty
        # if it was created earlier, there should be other ip addresses
        other_ips = self.zk.get(role_topic)

        # Zookeeper uses byte strings --> b'I'm a byte string'
        # We don't like that and need to convert it
        other_ips = codecs.decode(other_ips[0], 'utf-8')

        # if we just created the path, it will be an empty byte string
        # if it's empty, this will be true and we'll add our ip to the end of the other ips
        if other_ips != '':
            print("Adding to the topics list")
            self.zk.set(role_topic, codecs.encode(other_ips + ' ' + self.ipaddress, 'utf-8'))
        # else the byte string is empty and we can just send our ip_address
        else:
            self.zk.set(role_topic, codecs.encode(self.ipaddress, 'utf-8'))




