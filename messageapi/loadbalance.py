#
# Team 6
# Programming Assignment #3
#
# Contents:
#   - LoadProxy

import codecs
from collections import defaultdict
from math import floor
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
        self.registry["masters"] = defaultdict(list)
        self.setup_sockets()
        self.master_count = 1

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
            print("Listen - Check registry - Check Load")
            self.listen()
            self.checkRegistry()
            self.checkLoad()

    def listen(self):
        print("Listening")
        event = self.incoming_socket.poll(timeout=3000)  # wait 3 seconds
        if event == 0:
            # timeout reached before any events were queued
            pass
        else:
            #events queued within our time limit
            self.message = self.incoming_socket.recv_string()
            print("Message -> {}".format(self.message))
            role, topic, ipaddr = self.message.split()
            if ipaddr not in self.registry[role][topic]:
                self.registry[role][topic].append(ipaddr)
            # based on our role, we need to find the companion ip addresses in the registry
            if role == 'publisher' or role == 'subscriber':
                broker = self.get_primary_broker()
                print(broker)
                print(type(broker))
            if role == 'broker':
                broker = str(self.master_count)

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
            print("Rebalancing")
            self.rebalance()

    def rebalance(self):
        self.master_count = floor(len(self.registry[SUBSCRIBER]) + len(self.registry[PUBLISHER])/LOAD_THRESHOLD)
        print("Rebalancing threshold hit...")
        list_of_pubs = self.registry[PUBLISHER].keys()
        list_of_subs = self.registry[SUBSCRIBER].keys()
        pairs_of_topics = [topic for topic in list_of_pubs if topic in list_of_subs]
        # filter out pairs to leave list of pubs/subs without pairs
        unpaired_pubs = [topic for topic in list_of_pubs if topic not in pairs_of_topics]
        unpaired_subs = [topic for topic in list_of_subs if topic not in pairs_of_topics]
        # create index to divide the list of pairs in halves, rounded down
        dividing_index = floor(len(pairs_of_topics)/self.master_count)
        topics_data = pairs_of_topics[::]
        # split pairs in two
        for master in range(self.master_count):
            pairs = topics_data[:dividing_index]
            topics_data = topics_data[dividing_index:]
            self.registry['masters'][master] = pairs
            print(self.registry)
            ##################################
            # - brokers check in with load balancer when they're created
            # - get a number of masters back
            # - then the broker checks /broker/master
            # - and if they're aren't enough it registers
            # - load balancer gets a list of /broker/masters
            # - brokers then saves the split list to the registry

            # get broker 1
            # get broker 2
            # make sure that pubs and subs watch load balancer
            # then load balancer can kill itself, trigger everyone to get their new info
            # send pair_1 and subscriber to broker 1
            # send pair_2 and subscriber to broker 2
            # change load balance trigger to the next threshold?
            # which means you can also do maybe an iterative version of this
            # e.g. num_brokers++ instead of 2

            pass


