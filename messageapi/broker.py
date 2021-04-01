#
# Team 6
# Programming Assignment #3
#
# Contents:
#   - BrokerProxy
#   - BrokerPublisher
#   - BrokerSubscriber


import codecs
from collections import defaultdict
import json
import sys
import time

import zmq

from .util import local_ip4_addr_list
from .zeroroles import ZeroProxy, ZeroPublisher, ZeroSubscriber 
from .zeroroles import BROKER_PUBLISHER_PORT, BROKER_SUBSCRIBER_PORT, SERVER_ENDPOINT


##################################################################################
#
#
#           B R O K E R   P R O X Y 
#
#
####################################################################################

class BrokerProxy(ZeroProxy):
    def __init__(self):
        super().__init__()
        self.sockets = {}
        # Socket data maps to opposite port to X sockets (XPub, XSub)
        self.sockets_data = {"Subscriber": BROKER_PUBLISHER_PORT,
                            "Publisher": BROKER_SUBSCRIBER_PORT}
        self.setup_sockets()
        self.events = None

    def setup_sockets(self):
        for role, port in self.sockets_data.items():
            print("{} socket created.".format(role))
            #Based on the port being publisher odd, subscriber even
            zmq_socket = zmq.XPUB if (int(port) % 2) else zmq.XSUB
            self.sockets[role] = self.context.socket(zmq_socket)
            # Publisher gets an additional method Subscriber does not
            if zmq_socket == zmq.XPUB:
                self.sockets[role].setsockopt(zmq.XPUB_VERBOSE, 1)
            self.sockets[role].bind(SERVER_ENDPOINT.format(address="*", port=port))
            self.poller.register(self.sockets[role], zmq.POLLIN)
    
    def run(self):
        while True:
            try:
                self.poll()
                self.check_master_count()
            except NameError as e:
                print("Exception thrown: {}".format(sys.exc_info()[1]))

    def poll(self):
        # print ("Poll with a timeout of 1 sec")
        self.events = dict(self.poller.poll(1000))
        print("Events received = {}".format(self.events))
        for role, socket in self.sockets.items():
            #For each socket, get data
            self.get_socket_data(role)

    def get_socket_data(self, role):
        if self.sockets[role] in self.events:
            # We have an event -> receive the event
            msg = self.sockets[role].recv_string()
            print("{role} -> {message}".format(role=role, message=msg))
            # Send the message to the other role socket
            other_role = 'Subscriber' if role == 'Publisher' else 'Publisher'
            self.sockets[other_role].send_string(msg)


#############################################################
#
#
#           B R O K E R   P U B L I S H E R
#
#
##############################################################

class BrokerPublisher(ZeroPublisher):
    def __init__(self, topic):
        super().__init__(topic)
    
    def register(self):
        self.register_pub()

    def register_pub(self):
        #print("Publisher connecting to proxy at: {}".format(self.server_endpoint))
        self.socket = self.context.socket(zmq.PUB)
        self.socket.connect(self.server_endpoint)
    
    def publish(self, value):
        # print ("Message API Sending: {} {}".format(self.topic, value))
        message = {}
        message['topic'] = self.topic
        message['value'] = value
        message['time'] = time.time()
        self.socket.send_string("{topic} {time} {value}".format(**message))
    

################################################################################
#
#
#             B R O K E R   S U B S C R I B E R
#
#
################################################################################


class BrokerSubscriber(ZeroSubscriber):
    def __init__(self, topic):
        # ZooAnimal initialize
        super().__init__(topic)
        self.setup_sockets()

    def setup_sockets(self):
        self.socket = self.context.socket(zmq.SUB)

    def register(self):
        self.register_sub()

    def register_sub(self):
        print("Registering subscriber at: {}".format(self.server_endpoint))
        self.socket.connect(self.server_endpoint)
        self.socket.setsockopt_string(zmq.SUBSCRIBE, self.topic)

    # sub gets message
    def notify(self):
        message = self.socket.recv_string()
        # Split message based on our format
        topic, pub_time, *values = message.split()
        # get difference in time between now and when message was sent
        difference = time.time() - float(pub_time)
        # Write the difference in time from the publisher to the file
        with open("./logs/seconds_{}.log".format(self.ipaddress), "a") as f:
            f.write(str(difference) + "\n")
        return " ".join(values)


