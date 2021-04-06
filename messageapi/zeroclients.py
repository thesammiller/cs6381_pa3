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
from .zeroproxies import SERVER_ENDPOINT, BROKER_PUBLISHER_PORT, BROKER_SUBSCRIBER_PORT, FLOOD_PROXY_PORT, FLOOD_SUBSCRIBER_PORT

LOAD_BALANCE_PORT = '6666'

NO_REGISTERED_ENTRIES = ""

PATH_TO_MASTER_BROKER = "/broker/master"
PATH_TO_LOAD_BALANCER = "/load"

PUBLISHER = "publisher"
SUBSCRIBER = "subscriber"
BROKER = "broker"

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
        # Broker gives us the name of the node we need to use as our master
        print("BROKER ZNODE --> {}".format(self.master_znode))
        path = "/broker/" + self.master_znode
        # We then zk.get('/path/to/master0000000001') and that's how we get our ip address
        m_broker = self.zk.get(path, watch=self.broker_update)
        # here is where we're going to get the ip address
        print("BROKER IP --> {}".format(m_broker))
        self.broker = codecs.decode(m_broker[0], "utf-8")
        self.broker = json.loads(self.broker)
        self.server_endpoint = SERVER_ENDPOINT.format(address=self.broker['ip'], port=self.port)
        return self.broker

    def get_broker_from_load_balancer(self):
        for i in range(10):
            balances = self.zk.get_children(PATH_TO_LOAD_BALANCER)
            print("LOAD BALANCERS -> {}".format(balances))
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
                connect_str = SERVER_ENDPOINT.format(address=load_balance_address, port=LOAD_BALANCE_PORT)
                print("CONNECTING -> {}".format(connect_str))
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
                    print("REPLY RECEIVED -> {}".format(reply))
                    return reply
            time.sleep(0.2)

    def broker_update(self, data):
        print("Getting new master broker from ZooKeeper.")
        for i in range(10):
            try:
                self.broker = self.get_broker()
                print("Broker get -> {}".format(self.broker))
                self.server_endpoint = SERVER_ENDPOINT.format(
                    address=self.broker, port=self.port)
                self.register()
                break
            except:
                print("No master yet...")
            time.sleep(0.5)

    # children should use this as shell for register method
    def register_broker(self):
        print("Registering {} to address {}".format(self.role, self.broker['ip']))
        # Create handshake message for the Flood Proxy
        message = {}
        message['role'] = self.role
        message['topic'] = self.topic
        message['ipaddr'] = self.ipaddress
        hello_message = json.dumps(message)
        # Send to the proxy
        hello_socket = self.context.socket(zmq.REQ)
        print("Registering with the broker")
        print(hello_message)
        connect_str = SERVER_ENDPOINT.format(address=self.broker['ip'], port=self.port)
        hello_socket.connect(connect_str)
        hello_socket.send_string(hello_message)

        # Wait for return message
        event = hello_socket.poll(timeout=10000)  # wait 3 seconds
        if event == 0:
            # timeout reached before any events were queued
            print("TIMEOUT IN RESPONSE")
            raise Exception("No master.")
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
        self.broker = self.get_broker()
        self.server_endpoint = SERVER_ENDPOINT.format(
            address=self.broker['ip'], port=self.port)
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
        self.broker = self.get_broker()
        self.server_endpoint = SERVER_ENDPOINT.format(
            address=self.broker['ip'], port=self.port)

