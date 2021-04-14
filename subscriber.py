#!/usr/bin/python3

import json
import sys
from messageapi.broker import BrokerSubscriber
from messageapi.flood import FloodSubscriber


system = {"FLOOD": FloodSubscriber,
          "BROKER": BrokerSubscriber}

class WeatherSubscriber:

    def __init__(self, api, topic="00000",history="0"):
        self.sub = system[api](topic=topic, history=history)
        self.topic = topic
        self.sub.register_sub()

    def run(self):
        print("Running subscriber application...")
        total_temp = []
        while len(total_temp) < self.sub.history:
            message = self.sub.notify()
            if message is not None:
                message = self.sub.notify()
                # print("Suscriber Application got message.")
                # temperature, relhumidity = string.split(" ")
                try:
                    data = json.loads(message)
                    temperature = data['temperature']
                    total_temp.append(int(temperature))
                except:
                    pass
        print("Average temperature for zipcode '%s' was %dF" % (self.topic, sum(total_temp) / max(len(total_temp), 1)))


def main():
    topic_filter = sys.argv[1] if len(sys.argv) > 1 else "90210"
    api = sys.argv[2] if len(sys.argv) > 2 else "BROKER"
    history = sys.argv[3] if len (sys.argv) >3 else "5"
    
    if api not in system:
        print("Usage error -- message API can either be FLOOD or BROKER")
        sys.exit(-1)
    if not topic_filter.isdigit() or len(topic_filter) != 5:
        print("Usage error -- topic must be a zipcode (all numbers, 5 total).")
        sys.exit(-1)

    ws = WeatherSubscriber(api, topic=topic_filter, history=history)
    while True:
        ws.run()


if __name__ == "__main__":
    main()



