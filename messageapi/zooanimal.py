#
# Team 6
# Programming Assignment #2
#
# Contents:
#    - ZooAnimal
#

import codecs
import json
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


ZOOKEEPER_PATH_STRING = '/topic/{topic}/{role}'
PATH_TO_MASTER_BROKER = "/broker/master"



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
        self.zk_is_a_master = False

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

    # This is a function stub for the get_broker watch callback
    # The child is expected to implement their own logic
    # Pub and Sub need to register_sub()
    def broker_update(self, data):
        print("Broker updated.")
        print("Data -> {}".format(data))
        pass

    def get_broker(self):
        for i in range(10):
            if self.zk.exists(PATH_TO_MASTER_BROKER):
                node_data = self.zk.get(PATH_TO_MASTER_BROKER, watch=self.broker_update)
                broker_data = node_data[0]
                master_broker = codecs.decode(broker_data, 'utf-8')
                if master_broker != '':
                    self.broker = master_broker
                    return self.broker
                else:
                    raise Exception("No master broker.")
            time.sleep(0.2)

###########################################################################
            
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
            
#############################################################################################
            
            
class ZooProxy(ZooAnimal):
    def __init__(self):
        super().__init__()
        self.role = 'broker'
        self.topic = 'pool'
        self.zk_path = ZOOKEEPER_PATH_STRING.format(role=self.role, topic=self.topic)
        self.zookeeper_register()

    def zookeeper_register(self):
        if self.role == 'broker':
            broker_path = "/broker"
            data = {}
            data['ip'] = self.ipaddress
            data_string = json.dumps(data)
            encoded_ip = codecs.encode(data_string)
            if self.zk_seq_id == None:
                self.zk.create(self.zk_path, ephemeral=True, sequence=True, makepath=True, value=encoded_ip)
                brokers = self.zk.get_children(broker_path)
                brokers = [x for x in brokers if "lock" not in x]
                brokers = [x for x in brokers if "master" not in x]
                print(brokers)
                broker_nums = {y: int(y[4:]) for y in brokers}
                #sort based on the values
                broker_sort = sorted(broker_nums, key=lambda data: broker_nums[data])
                latest_id = broker_sort[-1]
                print(latest_id)
                self.zk_seq_id = latest_id
            for i in range(10):
                if self.zk.exists(broker_path+"/master") == None:
                    self.zookeeper_master()
                    break
                time.sleep(0.2)
            if self.zk.exists(broker_path + "/master"):
                # Get all the children
                path = self.zk.get_children(broker_path)
                # Remove the master
                
                #path.pop(path.index(self.zk_seq_id))
                # Process out the locks
                path = [x for x in path if "lock" not in x]
                path = [x for x in path if "master" not in x]
                #Convert into a dictionary of znode:sequential
                #We keep the path name as the key
                #Use the sequential number as the value
                # e.g. key pool000001 value 000001
                path_sort = sorted(path, key=lambda data: data[4:])
                previous = path_sort[path_sort.index(self.zk_seq_id)-1]
                watch_path = broker_path + "/" + previous
                self.zookeeper_watcher(watch_path)

##########################################################################################


class ZooClient(ZooAnimal):
    def __init__(self, role=None, topic="00000", history="5"):
        super().__init__()
        self.role = role
        self.topic = topic
        self.history = int(history)
        self.zk_seq_id = None
        self.zk_register()
        self.zk_ownership = self.zk_watch_owner()

    def zk_register(self):
        topic = "/topic/" + self.topic
        if not self.zk_seq_id:
            try:
                topic_clients = self.zk.get_children(topic)
                self.zk_ownership = len([x for x in topic_clients if self.role in x])
            except:
                self.zk_ownership = 0
            topic_role = topic + "/" + self.role
            json_data = {'ip': self.ipaddress, 'history': self.history, 'ownership': self.zk_ownership}
            json_string = json.dumps(json_data)
            json_encoded = codecs.encode(json_string, 'utf-8')
            self.zk.create(topic_role, ephemeral=True, makepath=True, sequence=True, value=json_encoded)
            topic_clients = self.zk.get_children(topic)
            topic_sort = sorted(topic_clients, key=lambda data: int(data[-5:]))
            self.zk_seq_id = topic_sort[-1]
            print("ZOOANIMAL ID -> {}".format(self.zk_seq_id))
        else:
            json_data = {'ip': self.ipaddress, 'history': self.history, 'ownership': self.zk_ownership}
            json_string = json.dumps(json_data)
            json_encoded = codecs.encode(json_string, 'utf-8')
            self.zk.set(topic + "/" + self.zk_seq_id, json_encoded)

    def zk_get_owner_position(self):
        topic = "/topic/" + self.topic
        topic_clients = self.zk.get_children(topic)
        topic_roles = [x for x in topic_clients if self.role in x]
        topic_sort = sorted(topic_roles, key=lambda data: int(data[-5:]))
        return topic_sort.index(self.zk_seq_id)

    def zk_watch_owner(self):
        self.zk_ownership = self.zk_get_owner_position()
        if self.zk_ownership != 0:
            topic = "/topic/" + self.topic
            topic_clients = self.zk.get_children(topic)
            topic_roles = [x for x in topic_clients if self.role in x]
            topic_sort = sorted(topic_roles, key=lambda data: int(data[-5:]))
            topic_index = topic_sort.index(self.zk_seq_id)
            self.zk.get(topic+'/'+topic_sort[topic_index-1], watch=self.zk_owner_reset)
        return self.zk_ownership

    def zk_owner_reset(self, data):
        self.zk_watch_owner()
        self.zk_register()

