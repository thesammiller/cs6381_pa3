#!/usr/bin/python3

import sys
from messageapi.broker import BrokerSubscriber
from messageapi.flood import FloodSubscriber


system = {"FLOOD": FloodSubscriber,
          "BROKER": BrokerSubscriber}

class WeatherSubscriber:

    def __init__(self, topic, api):
        self.sub = system[api](topic)
        self.topic = topic
        self.sub.register_sub()

    def run(self):
        print("Running subscriber application...")
        total_temp = 0
        for update_nbr in range(5):
            string = self.sub.notify()
            #print("Suscriber Application got message.")
            temperature, relhumidity = string.split(" ")
            total_temp += int(temperature)
            
        print("Average temperature for zipcode '%s' was %dF" % (self.topic, total_temp / (update_nbr+1)))

def main():

    topic_filter = sys.argv[1] if len(sys.argv) > 1 else "90210"
    api = sys.argv[2] if len(sys.argv) > 2 else "BROKER"
    
    if api not in system:
        print("Usage error -- message API can either be FLOOD or BROKER")
        sys.exit(-1)
    if not topic_filter.isdigit() or len(topic_filter) != 5:
        print("Usage error -- topic must be a zipcode (all numbers, 5 total).")
        sys.exit(-1)

        
    ws = WeatherSubscriber(topic_filter, api)
    while True:
        ws.run()

if __name__ == "__main__":
    main()



