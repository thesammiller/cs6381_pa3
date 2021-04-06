#
# Team 6
# Programming Assignment #3
#
# Contents:
#   - ZeroLoad
#   - ZeroProxy
#   - ZeroClient
#   - ZeroPublisher
#   - ZeroSubscriber
#

import codecs
import json
import time

import zmq

from .zooanimal import ZooLoad, ZooProxy, ZooClient

BROKER_PUBLISHER_PORT = "5556"
BROKER_SUBSCRIBER_PORT = "5555"

FLOOD_PROXY_PORT = "5555"
FLOOD_SUBSCRIBER_PORT = "5556"

LOAD_BALANCE_PORT = '6666'

SERVER_ENDPOINT = "tcp://{address}:{port}"
NO_REGISTERED_ENTRIES = ""

PATH_TO_MASTER_BROKER = "/broker/master"
PATH_TO_LOAD_BALANCER = "/load"

#############################
#
# ZMQ LOAD
#
#############################

class ZeroLoad(ZooLoad):
    def __init__(self):
        #ZooAnimal initialize
        super().__init__()
        # Zookeeper property
        self.role = 'load'
        self.topic = 'balance'
        self.masters = []
        #ZMQ Setup
        self.context = zmq.Context()
        self.poller = zmq.Poller()

    def setup_sockets(self):
        pass

    def run(self):
        pass
    def get_broker_list(self):
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

    def get_primary_broker(self):
        for i in range(100):
            if self.zk.exists("/broker"):
                node_data = self.zk.get_children("/broker")
                master_data = [m for m in node_data if "master" in m]
                print(master_data)
                if master_data is not None and master_data != []:
                    self.masters = master_data
                    print("################################################################")
                    print(self.masters)
                    print("################################################################")
                    #TODO: Better algorithm for choosing the primary broker
                    return self.masters[0]
                else:
                    raise Exception("No master broker.")
            time.sleep(0.2)


#############################
#
# ZMQ PROXY
#
#############################

class ZeroProxy(ZooProxy):
    def __init__(self):
        #ZooAnimal initialize
        super().__init__()
        # Zookeeper property
        self.role = 'broker'
        self.topic = 'pool'
        #ZMQ Setup
        self.context = zmq.Context()
        self.poller = zmq.Poller()
        self.number_of_masters = self.get_master_count_from_load_balancer()

    def check_master_count(self):
        self.number_of_masters = self.get_master_count_from_load_balancer()
        brokers = self.zk.get_children("/broker")
        masters = len([b for b in brokers if "master" in b])
        if masters < int(self.number_of_masters) and not self.zk_is_a_master:
            self.zookeeper_master()

    def setup_sockets(self):
        pass

    def run(self):
        pass

    def get_master_count_from_load_balancer(self):
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
                #hello_message = "{role} {topic} {ipaddr}".format(**message)
                hello_message = json.dumps(message)

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
                    print("MASTER COUNT -> {}".format(reply))
                    return reply
            time.sleep(0.2)
            return 0

#############################
#
# ZMQ CLIENT (BASE)
#
#############################

class ZeroClient(ZooClient):
    def __init__(self, role=None, topic=None, history=None):
        super().__init__(role=role, topic=topic, history=history)
        self.context = zmq.Context()
        self.port = 0
        self.socket = None
        self.master_znode = None

    def get_broker(self):
        while self.master_znode is None:
            self.master_znode = self.get_broker_from_load_balancer()
            time.sleep(0.1)
        #Broker gives us the name of the node we need to use as our master
        print("BROKER ZNODE --> {}".format(self.master_znode))
        path = "/broker/" + self.master_znode
        # We then zk.get('/path/to/master0000000001') and that's how we get our ip address
        m_broker = self.zk.get(path, watch=self.broker_update)
        print("BROKER IP --> {}".format(m_broker))  # here is where we're going to get the ip address
        self.broker = codecs.decode(m_broker[0], "utf-8")
        self.server_endpoint = SERVER_ENDPOINT.format(address=self.broker, port=self.port)
        return self.broker

    def get_broker_from_load_balancer(self):
        for i in range(10):
            balances = self.zk.get_children(PATH_TO_LOAD_BALANCER)
            print(balances)
            if balances:
                load_balancer = balances[0]
                load_balance_path = PATH_TO_LOAD_BALANCER + "/" + load_balancer
                load_balance_address = codecs.decode(self.zk.get(load_balance_path)[0], "utf-8")
                message = {}
                message['role'] = self.role
                message['topic'] = self.topic
                message['ipaddr'] = self.ipaddress
                # role=self.role, topic=self.topic, ipaddr=self.ipaddress
                hello_message = json.dumps(message)
                #hello_message = "{role} {topic} {ipaddr}".format(**message)

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
    def register_broker(self):
        print("{} - > Registering {} to address {}".format(self.zk_path, self. role, self.broker))

        # Create handshake message for the Flood Proxy
        message = {}
        message['role'] = self.role
        message['topic'] = self.topic
        message['ipaddr'] = self.ipaddress
        hello_message = json.dumps(message)

        # Send to the proxy
        hello_socket = self.context.socket(zmq.REQ)
        print("Registering with the broker")
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
            print("REGISTER BROKER --> {}".format(reply))
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
    def __init__(self, topic=None, history=None):
        super().__init__(role="publisher", topic=topic, history=history)
        self.port = BROKER_PUBLISHER_PORT
        self.server_endpoint = SERVER_ENDPOINT.format(address=self.broker, port=self.port)
        self.broker = self.get_broker()
        print("Zero Publisher")


#############################
#
# ZMQ SUBSCRIBER
# 
###########################

class ZeroSubscriber(ZeroClient):
    def __init__(self, topic=None, history=None):
        super().__init__(role="subscriber", topic=topic, history=history)
        self.role = 'subscriber'
        self.port = BROKER_SUBSCRIBER_PORT
        self.server_endpoint = SERVER_ENDPOINT.format(address=self.broker, port=self.port)
        self.broker = self.get_broker()

