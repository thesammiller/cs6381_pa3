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
            print("Becoming the master.")
            role_topic = ZOOKEEPER_PATH_STRING.format(role=self.role, topic='master')
            encoded_ip = codecs.encode(self.ipaddress, "utf-8")
            self.zk.create(role_topic, ephemeral=True, makepath=True, value=encoded_ip)
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
            encoded_ip = codecs.encode(self.ipaddress)
            if self.zk_seq_id == None:
                self.zk.create(self.zk_path, ephemeral=True, sequence=True, makepath=True, value=encoded_ip)
                brokers = self.zk.get_children(broker_path)
                try:
                    brokers.pop(brokers.index("master"))
                except:
                    pass
                brokers = [x for x in brokers if "lock" not in x]
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
                path.pop(path.index("master"))
                #path.pop(path.index(self.zk_seq_id))
                # Process out the locks
                path = [x for x in path if "lock" not in x]
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
    def __init__(self, role, topic):
        super().__init__()
        self.role = role
        self.topic = topic
        self.zookeeper_register()


    def zookeeper_register(self):
     # This will result in a path of /broker/publisher/12345 or whatever
        # or /broker/broker/master
        role_topic = ZOOKEEPER_PATH_STRING.format(role=self.role, topic=self.topic)
        print("Zooanimal IP-> {}".format(self.ipaddress))
        encoded_ip = codecs.encode(self.ipaddress, "utf-8")

        if self.role =='publisher' or self.role=='subscriber':
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

    
