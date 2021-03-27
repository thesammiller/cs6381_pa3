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

SERVER_ENDPOINT = "tcp://{address}:{port}"
NO_REGISTERED_ENTRIES = ""

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
        self.broker = self.get_broker()
        self.port = 0
        self.socket = None

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


