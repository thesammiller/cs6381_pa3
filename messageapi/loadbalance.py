#
# Team 6
# Programming Assignment #3
#
# Contents:
#   - LoadProxy

import codecs
from collections import defaultdict
import json
from pprint import pprint as print
import random
import time

import zmq

from .zeroroles import ZeroLoad, SERVER_ENDPOINT, LOAD_BALANCE_PORT

BROKER = "broker"
PUBLISHER = "publisher"
SUBSCRIBER = "subscriber"

NO_REGISTERED_ENTRIES = ""

# LOAD THRESHOLD VARIABLE -- first and second
LOAD_THRESHOLDS = [8, 16]


###############################################################
#  L O A D  P R O X Y
################################################################

class LoadProxy(ZeroLoad):
    def __init__(self):
        # ZooAnimal
        self.role = 'load'
        self.topic = 'balance'
        super().__init__()
        # Initialize Registry -- if there is a key error, it will create a dictionary that creates lists
        self.registry = defaultdict(lambda: defaultdict(list))
        self.setup_sockets()
        self.master_count = 1
        self.threshold_index = 0
        self.topic_brokers = {}
        self.do_rebalance = True

    def setup_sockets(self):
        print("Setup sockets")
        self.incoming_socket = self.context.socket(zmq.REP)
        # creating a server bound to port 5555
        self.incoming_socket.bind(SERVER_ENDPOINT.format(
            address="*", port=LOAD_BALANCE_PORT))

    # encloses basic functionality
    # is a difference in the file
    def run(self):
        print("Running")
        while True:
            #print("Listen - Check registry - Check Load")
            self.listen()
            self.check_registry()
            self.check_load()

    def listen(self):
        # print("Listening")
        event = self.incoming_socket.poll(timeout=3000)  # wait 3 seconds
        if event == 0:
            # timeout reached before any events were queued
            pass
        else:
            # events queued within our time limit
            message = self.incoming_socket.recv_string()
            print("Message -> {}".format(message))
            #role, topic, ipaddr = self.message.split()
            data = json.loads(message)
            role = data['role'] #publisher
            topic = data['topic'] #12345
            ipaddr = data['ipaddr'] #10.0.0.1
            if ipaddr not in self.registry[topic][role]: #is 10.0.0.1 in registry['12345']['publisher']
                self.registry[topic][role].append(ipaddr) #now it's added to the list ['10....1']
            # based on our role, we need to find the companion ip addresses in the registry
            if role == 'publisher' or role == 'subscriber':
                if topic in self.topic_brokers:
                    broker = self.topic_brokers[topic]
                else:
                    for i in range(10):
                        try:
                            self.update_broker_registry('/broker')
                            # Choose a broker at random if we don't know the topic
                            broker = random.choice(self.registry['broker'])
                            self.topic_brokers[topic] = broker
                        except:
                            time.sleep(0.1)
            if role == 'broker':
                broker = str(self.master_count)
            self.incoming_socket.send_string(broker)

    def update_client_registry(self, path):
        #print("Updating pub-sub registry...")
        try:
            topic_list = self.zk.get_children(path)
        except:
            return
        for topic in topic_list:
            children = self.zk.get_children(path+'/'+topic)
            for entry in children:
                try:
                    data = self.zk.get(path + '/{}'.format(entry))[0]
                    decoded_data = codecs.decode(data, 'utf-8')
                    list_of_addresses = decoded_data.split()
                    role = PUBLISHER if PUBLISHER in entry else SUBSCRIBER
                    self.registry[topic][role] = list_of_addresses
                except:
                    continue

    def update_broker_registry(self, path):
        #print("Updating registry...")
        children = self.zk.get_children(path)
        masters = [m for m in children if 'master' in m]
        print("Updated Masters --> {}".format(masters))
        self.registry[path[1:]] = masters

    def check_registry(self):
        #print("Checking registry...")
        # get all the /flood/subscriber children
        self.update_client_registry("/topic")
        self.update_broker_registry('/broker')

    def check_load(self):
        # sub_topics = self.registry[SUBSCRIBER]
        # sub_scribers = sum(sub_topics)
        if len(self.registry[SUBSCRIBER]) + len(self.registry[PUBLISHER]) > LOAD_THRESHOLDS[self.threshold_index] & self.do_rebalance:
            print("Rebalancing")
            self.rebalance()
            self.threshold_index += 1
            if self.threshold_index >= len(LOAD_THRESHOLDS):
                self.threshold_index = len(LOAD_THRESHOLDS)-1

    def rebalance(self):
        self.topic_brokers = {}
        time.sleep(1)
        self.check_registry()
        if self.threshold_index == len(LOAD_THRESHOLDS)-1:
            self.do_rebalance = False
