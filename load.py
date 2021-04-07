#!/usr/bin/python3

import sys
from messageapi.loadbalance import LoadProxy


def main():

    print("Starting LoadProxy...")
    proxy = LoadProxy()
    print("LoadProxy initialized.")
    proxy.run()
    print("This should not be visible because Proxy is running.")


if __name__ == '__main__':
    main()
