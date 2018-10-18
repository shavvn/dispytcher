#!/usr/bin/env python3

"""
The worker process.

Each worker should be running on one and only one actual server.
It receives configurations from dispatcher and run the jobs.
So yeah, a worker is actually a server from jsonsocket...
"""

import argparse
import socket
from jsonsocket import Server


def init_server(host_name, port):
    """Initialize a server process

    Arguments:
        host_name {string} -- [host ip]
        port {int} -- [socket port]

    Raise:
    Returns:
        Server -- a server object

    Raises:
        OSError -- happens when resolving host or creating socket
    """

    host_ip = socket.gethostbyname(host_name)
    server = Server(host_ip, port)
    return server


def run(server):
    """Start running a server process

    Arguments:
        server {Server} -- socket server
    Returns:
        bool -- doesn't matter...
    """

    while True:
        server.accept()  # blocking
        try:
            data = server.recv()
        except (ValueError, OSError) as e:
            print('Cannot recv data! Closing socket...')
            print(e.message)
            # forcing client to close to free up resource
            continue
        except Exception as e:
            print('Unexpected error!')
            print(e.message)
            continue
        print(data)
    return True


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser(description='Initialize worker')
    arg_parser.add_argument('hostname', type=str,
                            help='external accessible hostname of this worker')
    arg_parser.add_argument('-p', '--port', type=int, default=6666)

    args = arg_parser.parse_args()
    if args.port <= 1024:  # kernel port
        print("try a port # larger than 1024!")
        exit(1)
    host_name = args.hostname
    server = init_server(host_name, args.port)
    run(server)
