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

from .zeroroles import ZeroProxy, ZeroPublisher, ZeroSubscriber 
from .zeroroles import BROKER_PUBLISHER_PORT, BROKER_SUBSCRIBER_PORT, SERVER_ENDPOINT


SPLITSTRING = "----"

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
    def __init__(self, topic="00000", history="5"):
        # ZooAnimal initialize
        super().__init__(topic=topic, history=history)
        self.history_list = []

    def add_to_history(self, data):
        self.history_list.append(data)
        if len(self.history_list) > self.history:
            self.history_list = self.history_list[1:]
    
    def register(self):
        #self.register_with_broker()
        self.register_pub()

    def register_pub(self):
        #print("Publisher connecting to proxy at: {}".format(self.server_endpoint))
        self.socket = self.context.socket(zmq.PUB)
        self.socket.connect(self.server_endpoint)
    
    def publish(self, data):
        self.broker_update('')
        self.add_to_history(data)
        if self.broker != None:
            # print ("Message API Sending: {} {}".format(self.topic, value))
            for d in self.history_list:
                message = {}
                message['topic'] = self.topic
                message['data'] = d
                message['time'] = time.time()
                message['seq_id'] = self.zk_seq_id
                message['split'] = SPLITSTRING
                #message_string = json.dumps(message)
                #print(message_string)
                message_string = "{topic}{split}{time}{split}{seq_id}{split}{data}".format(**message)
                self.socket.send_string(message_string)
    

################################################################################
#
#
#             B R O K E R   S U B S C R I B E R
#
#
################################################################################


class BrokerSubscriber(ZeroSubscriber):
    def __init__(self, topic="00000", history="5"):
        # ZooAnimal initialize
        super().__init__(topic=topic, history=history)
        self.pub_owner = None
        self.setup_sockets()

    def setup_sockets(self):
        self.socket = self.context.socket(zmq.SUB)

    def register(self):
        #self.register_with_broker()
        self.register_sub()

    def register_sub(self):
        print("Registering subscriber at: {}".format(self.server_endpoint))
        self.socket.connect(self.server_endpoint)
        self.socket.setsockopt_string(zmq.SUBSCRIBE, self.topic)

    def notify(self):
        self.broker_update('')
        message = self.socket.recv_string()
        # Split message based on our format
        topic, pub_time, seq_id, data = message.split(SPLITSTRING)
        zk_path = "/topic/{topic}/{seq_id}".format(topic=topic, seq_id=seq_id)
        zk_data = self.zk.get(zk_path)
        zk_decode = codecs.decode(zk_data[0])
        zk_json = json.loads(zk_decode)
        ownership = zk_json['ownership']
        pub_history = zk_json['history']
        if ownership == 0 and pub_history >= self.history:
            self.pub_owner = zk_json['ip']
        elif ownership == 0 and pub_history <= self.history:
            clients = self.zk.get_children('/topic/'+self.topic)
            publishers = [c for c in clients if "publisher" in c]
            sorted_publishers = sorted(publishers, key=lambda p: int(p[-6:]))
            for p in sorted_publishers:
                data = self.zk.get("/topic/" + self.topic + '/' + p)
                data_decoded = codecs.decode(data[0])
                data_json = json.loads(data_decoded)
                if data_json['history'] >= self.history:
                    self.pub_owner = data_json['ip']
                    break
        if zk_json['ip'] == self.pub_owner:
            difference = time.time() - float(pub_time)
            # Write the difference in time from the publisher to the file
            with open("./logs/seconds_{}.log".format(self.ipaddress), "a") as f:
                f.write(str(difference) + "\n")
            return data
        # Subscribers should check for none returned
        else:
            return None
