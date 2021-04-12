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

from .zeroroles import ZeroProxy, ZeroPublisher, ZeroSubscriber 
from .zeroroles import FLOOD_PROXY_PORT, FLOOD_SUBSCRIBER_PORT, SERVER_ENDPOINT

FLOOD_PUBLISHER = "publisher"
FLOOD_SUBSCRIBER = "subscriber"

NO_REGISTERED_ENTRIES = ""


#########################################################
#
#            F L O O D   P R O X Y
#
################################################################

class FloodProxy(ZeroProxy):
    def __init__(self):
        # ZooAnimal
        super().__init__()
        # Initialize Registry
        self.registry = {}
        self.registry[FLOOD_PUBLISHER] = defaultdict(list)
        self.registry[FLOOD_SUBSCRIBER] = defaultdict(list)
        self.setup_sockets()

    def setup_sockets(self):
        self.incoming_socket = self.context.socket(zmq.REP)
        # creating a server bound to port 5555
        self.incoming_socket.bind(SERVER_ENDPOINT.format(address="*", port=FLOOD_PROXY_PORT))

    # encloses basic functionality
    def run(self):
        while True:
            self.listen()
            self.checkRegistry()

    def listen(self):
        self.message = self.incoming_socket.recv_string()
        print("Message -> {}".format(self.message))
        role, topic, ipaddr = self.message.split()
        if ipaddr not in self.registry[role][topic]:
            self.registry[role][topic].append(ipaddr)
        # based on our role, we need to find the companion ip addresses in the registry
        other = FLOOD_SUBSCRIBER if role == FLOOD_PUBLISHER else FLOOD_PUBLISHER
        if self.registry[other][topic]:
            result = " ".join(self.registry[other][topic])
        else:
            result = NO_REGISTERED_ENTRIES
        print("REGISTRY -> {}".format(self.registry))
        self.incoming_socket.send_string(result)

    def update_registry(self, path):
        children = self.zk.get_children(path)
        for entry in children:
            data = self.zk.get(path + '/{}'.format(entry))[0]
            decoded_data = codecs.decode(data, 'utf-8')
            print("{path} -> {data}".format(path=path, data=decoded_data))
            list_of_addresses = decoded_data.split()
            # path[1:] -> /publisher becomes publisher; /subscriber becomes subscriber
            key = path[1:]
            self.registry[path[1:]][entry] = list_of_addresses

    def checkRegistry(self):
        if self.zk.get("/broker/master")[0] == codecs.encode(self.ipaddress, 'utf-8'): # And we are the master.
            # get all the /flood/subscriber children
            self.update_registry("/subscriber")
            self.update_registry("/publisher")


############################################################################################
#
#
#            F L O O D   P U B L I S H E R
#
#
##########################################################################################

class FloodPublisher(ZeroPublisher):

    def __init__(self, topic):
        # Initialize ZooAnimal
        super().__init__(topic)
        # ZooAnimal Properties
        self.zk_path = ZOOKEEPER_PATH_STRING.format(role=self.role, topic=self.topic)
        # ZMQ Setup
        self.context.setsockopt(zmq.LINGER, 10)
        # API Setup
        self.registry = []
        self.message = ""

    def register_pub(self):
        self.register()

    def publish(self, data):
        print("{} -> Publishing...".format(self.zk_path))
        self.register_pub()
        for ipaddr in self.registry:
            print("{} -> Address {}".format(self.zk_path, ipaddr))
            seconds = time.time()
            self.socket = self.context.socket(zmq.REQ)
            self.connect_str = SERVER_ENDPOINT.format(address=ipaddr, port=FLOOD_SUBSCRIBER_PORT)
            #print(self.connect_str)
            self.socket.connect(self.connect_str)
            self.message = "{time} {data}".format(time=seconds, data=data)
            #print(self.message)
            self.socket.send_string(self.message)
            reply = self.socket.recv_string()

###################################################################################################################
#
#
#            F L O O D    S U B S C R I B E R
#
#
####################################################################################################################

class FloodSubscriber(ZeroSubscriber):
    def __init__(self, topic):
        # Initialize ZooAnimal
        super().__init__(topic)
        # ZooAnimal Properties
        self.zk_path = ZOOKEEPER_PATH_STRING.format(role=self.role, topic=self.topic)
        # ZMQ Setup
        self.setup_sockets()

    def setup_sockets(self):
        self.socket = self.context.socket(zmq.REP)
        self.socket.bind(SERVER_ENDPOINT.format(address="*", port=FLOOD_SUBSCRIBER_PORT))

    #def register(self):
    #    self.register_sub()

    def register_sub(self):
        self.register()

    def notify(self):
        print("{} -> Waiting for notification".format(self.zk_path))
        self.message = self.socket.recv_string()
        # Write to file with time difference from sent to received
        seconds = time.time()
        pub_time, *values = self.message.split()
        difference = seconds - float(pub_time)
        with open("logs/{ip}.log".format(ip=self.ipaddress), "a") as f:
            f.write(str(difference) + "\n")

        print("{zk_path} -> Subscriber received data {data}".format(zk_path=self.zk_path, data=" ".join(values)))
        self.socket.send_string(self.message)
        return " ".join(values)
