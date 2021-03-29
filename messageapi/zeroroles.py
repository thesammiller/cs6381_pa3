#
# Team 6
# Programming Assignment #2
#
# Contents:
#   - BrokerProxy
#   - BrokerPublisher
#   - BrokerSubscriber
#
#   - FloodProxy
#   - FloodPublisher
#   - FloodSubscriber

# Standard Library
import codecs
from collections import defaultdict
import json
import sys
import time

# Third Party
import zmq

# Local
from .util import local_ip4_addr_list
from .zooanimal import ZooAnimal, ZOOKEEPER_ADDRESS, ZOOKEEPER_PORT, ZOOKEEPER_PATH_STRING

BROKER_PUBLISHER_PORT = "5556"
BROKER_SUBSCRIBER_PORT = "5555"

FLOOD_PROXY_PORT = "5555"
FLOOD_SUBSCRIBER_PORT = "5556"

LOAD_BALANCE_PORT = '6666'

SERVER_ENDPOINT = "tcp://{address}:{port}"
NO_REGISTERED_ENTRIES = ""

PATH_TO_MASTER_BROKER = "/broker/master"
PATH_TO_LOAD_BALANCER = "/load"

class ZeroLoad(ZooAnimal):
    def __init__(self):
        #ZooAnimal initialize
        super().__init__()
        # Zookeeper property
        self.role = 'load'
        self.topic = 'balance'
        #ZMQ Setup
        self.context = zmq.Context()
        self.poller = zmq.Poller()
        self.zookeeper_register()

    def setup_sockets(self):
        pass

    def run(self):
        pass

    def get_primary_broker(self):
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

class ZeroProxy(ZooAnimal):
    def __init__(self):
        #ZooAnimal initialize
        super().__init__()
        # Zookeeper property
        self.role = 'broker'
        self.topic = 'pool'
        #ZMQ Setup
        self.context = zmq.Context()
        self.poller = zmq.Poller()
        self.zookeeper_register()

    def setup_sockets(self):
        pass

    def run(self):
        pass

#############################
#
# ZMQ CLIENT (BASE)
#
#############################

class ZeroClient(ZooAnimal):
    def __init__(self, topic):
        super().__init__()
        self.topic = topic
        self.context = zmq.Context()
        self.port = 0
        self.socket = None

    def get_broker(self):
        self.broker = self.get_broker_from_load_balancer()
        print("BROKER --> {}".format(self.broker))
        self.server_endpoint = SERVER_ENDPOINT.format(address=self.broker, port=self.port)
        return self.broker


    def get_broker_from_load_balancer(self):
        for i in range(10):
            balances = self.zk.get_children(PATH_TO_LOAD_BALANCER)
            if balances:
                load_balancer = balances[0]
                load_balance_path = PATH_TO_LOAD_BALANCER + "/" + load_balancer
                load_balance_address = codecs.decode(self.zk.get(load_balance_path)[0], "utf-8")
                message = {}
                message['role'] = self.role
                message['topic'] = self.topic
                message['ipaddr'] = self.ipaddress
                # role=self.role, topic=self.topic, ipaddr=self.ipaddress
                hello_message = "{role} {topic} {ipaddr}".format(**message)

                hello_socket = self.context.socket(zmq.REQ)
                print(self.broker)
                connect_str = SERVER_ENDPOINT.format(address=load_balance_address, port=LOAD_BALANCE_PORT)
                print(connect_str)
                hello_socket.connect(connect_str)
                hello_socket.send_string(hello_message)
                # Wait for return message
                event = hello_socket.poll(timeout=3000)  # wait 3 seconds
                if event == 0:
                    # timeout reached before any events were queued
                    pass
                else:
                    #    events queued within our time limit
                    reply = hello_socket.recv_string(flags=zmq.NOBLOCK)
                    return reply
            time.sleep(0.2)

    def get_primary_broker(self):
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

    def broker_update(self, data):
        print("Getting new master broker from ZooKeeper.")
        for i in range(10):
            try:
                self.broker = self.get_broker()
                print("Broker get -> {}".format(self.broker))
                self.server_endpoint = SERVER_ENDPOINT.format(address=self.broker, port=self.port)
                self.register()
                break
            except:
                print("No master yet...")
            time.sleep(0.5)

    #children should use this as shell for register method
    def register(self):
        print("{} - > Registering {} to address {}".format(self.zk_path, self. role, self.broker))
        # Create handshake message for the Flood Proxy
        message = {}
        message['role'] = self.role
        message['topic'] = self.topic
        message['ipaddr'] = self.ipaddress
        hello_message = "{role} {topic} {ipaddr}".format(**message)
        # Send to the proxy
        hello_socket = self.context.socket(zmq.REQ)
        print(self.broker)
        connect_str = SERVER_ENDPOINT.format(address=self.broker, port=FLOOD_PROXY_PORT)
        hello_socket.connect(connect_str)
        hello_socket.send_string(hello_message)
        # Wait for return message
        event = hello_socket.poll(timeout=3000)  # wait 3 seconds
        if event == 0:
        # timeout reached before any events were queued
            pass
        else:
        #    events queued within our time limit
            reply = hello_socket.recv_string(flags=zmq.NOBLOCK)
            print(reply)
            if reply != NO_REGISTERED_ENTRIES:
                self.registry = reply.split()
                print("{zk_path} -> Received new registry: {registry}".format(zk_path=self.zk_path,
                                                                          registry=self.registry))
            if reply == NO_REGISTERED_ENTRIES:
                print("No entries.")
                self.registry = []    

##########################
#
# ZMQ PUBLISHER
#
##########################

class ZeroPublisher(ZeroClient):
    def __init__(self, topic):
        super().__init__(topic)
        self.role = 'publisher'
        self.port = BROKER_PUBLISHER_PORT
        self.server_endpoint = SERVER_ENDPOINT.format(address=self.broker, port=self.port)
        self.zookeeper_register()
        self.broker = self.get_broker()


#############################
#
# ZMQ SUBSCRIBER
# 
###########################

class ZeroSubscriber(ZeroClient):
    def __init__(self, topic):
        super().__init__(topic)
        self.role = 'subscriber'
        self.port = BROKER_SUBSCRIBER_PORT
        self.server_endpoint = SERVER_ENDPOINT.format(address=self.broker, port=self.port)
        self.zookeeper_register()
        self.broker = self.get_broker()


