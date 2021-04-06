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
# ZMQ LOAD
#

class ZeroLoad(ZooLoad):
    def __init__(self):
        super().__init__()
        self.masters = []
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
                    print(self.masters)
                    # TODO: Better algorithm for choosing the primary broker
                    return self.masters[0]
                else:
                    raise Exception("No master broker.")
            time.sleep(0.2)

#############################
# ZMQ PROXY
#

class ZeroProxy(ZooProxy):
    def __init__(self):
        super().__init__()
        self.context = zmq.Context()
        self.poller = zmq.Poller()
        self.number_of_masters = self.get_master_count_from_load_balancer()

    def check_master_count(self):
        self.master_count = self.get_master_count_from_load_balancer()
        brokers = self.zk.get_children("/broker")
        num_masters = len([b for b in brokers if "master" in b])
        if num_masters < int(self.master_count):
            if not self.zk_is_a_master:
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
                load_balance_address = codecs.decode(
                    self.zk.get(load_balance_path)[0], "utf-8")

                message = {}
                message['role'] = self.role
                message['topic'] = self.topic
                message['ipaddr'] = self.ipaddress
                hello_message = json.dumps(message)
                
                hello_socket = self.context.socket(zmq.REQ)
                connect_str = SERVER_ENDPOINT.format(address=load_balance_address, port=LOAD_BALANCE_PORT)
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
        


