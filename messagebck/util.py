import socket
import fcntl
import os
import struct


def local_ip4_addr_list():
    """Return a set of IPv4 address
    """
    assert os.name != 'nt', 'Do not support Windows rpc yet.'
    nic = set()
    for if_nidx in socket.if_nameindex():
        name = if_nidx[1]
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            ip_of_ni = fcntl.ioctl(sock.fileno(),
                                   0x8915,  # SIOCGIFADDR
                                   struct.pack('256s', name[:15].encode("UTF-8")))
        except OSError as e:
            if e.errno == 99: # EADDRNOTAVAIL
                print("Warning!",
                      "Interface: {}".format(name),
                      "IP address not available for interface.",
                      sep='\n')
                continue
            else:
                raise e

        ip_addr = socket.inet_ntoa(ip_of_ni[20:24])
        nic.add(ip_addr)
    return nic 

def main():
    print(local_ip4_addr_list())
