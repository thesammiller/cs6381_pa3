#
# Team 6
# Programming Assignment #3
#
# Contents:
#   - LoadProxy

# Standard Library
import codecs
from collections import defaultdict
from pprint import pprint as print
import json
import sys
import time

# Third Party
import zmq

# Local
from .util import local_ip4_addr_list

from .zeroroles import ZeroLoad, SERVER_ENDPOINT, LOAD_BALANCE_PORT

BROKER = "broker"
PUBLISHER = "publisher"
SUBSCRIBER = "subscriber"

NO_REGISTERED_ENTRIES = ""
LOAD_THRESHOLD = 2



###############################################################
#
#            L O A D  P R O X Y
#
################################################################

class LoadProxy(ZeroLoad):
    def __init__(self):
        # ZooAnimal
        self.role = 'load'
        self.topic = 'balance'
        super().__init__()
        # Initialize Registry
        self.registry = {}
        self.registry[BROKER] = defaultdict(list)
        self.registry[PUBLISHER] = defaultdict(list)
        self.registry[SUBSCRIBER] = defaultdict(list)
        self.setup_sockets()

    def setup_sockets(self):
        print("Setup sockets")
        self.incoming_socket = self.context.socket(zmq.REP)
        # creating a server bound to port 5555
        self.incoming_socket.bind(SERVER_ENDPOINT.format(address="*", port=LOAD_BALANCE_PORT))

    # encloses basic functionality
    # is a difference in the file
    def run(self):
        print("Running")
        while True:
            print("Waiting for message...")
            self.listen()
            print("Message received...")
            self.checkRegistry()
            print(self.registry)
            self.checkLoad()

    def listen(self):
        print("Listening")
        self.message = self.incoming_socket.recv_string()
        print("Message -> {}".format(self.message))
        role, topic, ipaddr = self.message.split()
        if ipaddr not in self.registry[role][topic]:
            self.registry[role][topic].append(ipaddr)
        # based on our role, we need to find the companion ip addresses in the registry
        broker = self.get_primary_broker()
        self.incoming_socket.send_string(broker)

    def update_registry(self, path):
        print("Updating registry...")
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
        print("Checking registry...")
        # get all the /flood/subscriber children
        self.update_registry("/subscriber")
        self.update_registry("/publisher")
        self.update_registry('/broker')

    def checkLoad(self):
        if len(self.registry[SUBSCRIBER]) + len(self.registry[PUBLISHER]) > LOAD_THRESHOLD:
            self.rebalance()

    def rebalance(self):
        print("Rebalancing threshold hit...")
        pass


