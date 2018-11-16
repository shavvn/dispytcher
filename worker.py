#!/usr/bin/env python3

"""
The worker process.

Each worker should be running on one and only one actual server.
It receives configurations from dispatcher and run the jobs.
So yeah, a worker is actually a server from jsonsocket...
"""

import argparse
import socket
import subprocess
import threading
from jsonsocket import Server


class Worker(object):

    def __init__(self, host_name, port):
        """Initialize a server process

        Arguments:
            host_name {string} -- [host ip]
            port {int} -- [socket port]

        Raises:
            OSError -- happens when resolving host or creating socket
        """
        host_ip = socket.gethostbyname(host_name)
        self.server = Server(host_ip, port)
        self._threads = []
        self._thread_stops = []
        self._pending_procs = {}

    def run(self):
        while True:
            self.server.accept()  # blocking
            try:
                data = self.server.recv()
            except (ValueError, OSError) as e:
                print('Cannot recv data! Closing socket...')
                print(e.message)
                # forcing client to close to free up resource
                continue
            except Exception as e:
                print('Unexpected error!')
                print(e.message)
                continue
            if data['action'] == 'stop':
                self.stop()
            else:
                stop_event = threading.Event()
                thread = threading.Thread(
                    target=self.execute, args=(data, stop_event,))
                self._thread_stops.append(stop_event)
                thread.start()
        return True

    def execute(self, data, stop_event):
        cmds = []
        for cmd_data in data['cmds']:
            cmd = [cmd_data['cmd']]
            if cmd_data.get('args'):
                cmd += cmd_data['args']
            cmds.append(cmd)

        print("Job:", data["name"])
        if data['action'] == 'dry':
            for cmd in cmds:
                print(cmd)
        else:
            for cmd in cmds:
                proc = subprocess.Popen(cmd)
                self._pending_procs[proc.pid] = proc
                proc.wait()
                self._pending_procs.pop(proc.pid)
                if stop_event.is_set():
                    break

    def stop(self):
        for event in self._thread_stops:
            event.set()
        self._thread_stops.clear()
        for thread in self._threads:
            thread.join()
        self._threads.clear()
        for pid, proc in self._pending_procs.items():
            if proc.returncode is not None:
                proc.terminate()


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
    worker = Worker(host_name, args.port)
    try:
        worker.run()
    except KeyboardInterrupt:
        print('terminating...')
        worker.stop()
        exit(0)
