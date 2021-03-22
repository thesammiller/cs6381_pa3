#!/usr/bin/python3

import sys
from messageapi.broker import BrokerProxy
from messageapi.flood import FloodProxy

#to be run on 10.0.0.1

system = {"FLOOD": FloodProxy, "BROKER": BrokerProxy}

def main():
    approach = sys.argv[1] if len(sys.argv) > 1 else "BROKER"

    if approach not in system.keys():
        print("Usage error -- proxy can either be FLOOD or BROKER")
        sys.exit(-1)

    proxy_system = system[approach]

    print("Starting Proxy...")
    proxy = proxy_system()
    print("Proxy initialized.")
    proxy.run()
    print("This should not be visible because Proxy is running.")

if __name__ == '__main__':
    main()



        



       



