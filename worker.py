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

        # internal book keeping of running jobs
        self._job_id = 0
        self._threads = {}
        self._thread_stops = {}
        self._pending_procs = {}

    def start(self):
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

            action = data.get('action')
            if action == 'stop':
                self.stop()
            elif action == 'dry':
                self.dry_run(data)
            elif action == 'run':
                stop_event = threading.Event()
                thread = threading.Thread(
                    target=self.execute,
                    args=(data, self._job_id, stop_event,)
                )
                self._thread_stops[self._job_id] = stop_event
                self._threads[self._job_id] = thread
                thread.start()
                self._job_id += 1
            else:
                continue
        return True

    def prep_cmds(self, data):
        """Given data prepare the list of commands

        Each command is a list of program + args that can be put into a
        subprocess call.
        Arguments:
            data {dict} -- job data

        Returns:
            [list] -- list of commands
        """
        cmds = []
        for cmd_data in data['cmds']:
            cmd = [cmd_data['cmd']]
            if cmd_data.get('args'):
                cmd += cmd_data['args']
            cmds.append(cmd)
        return cmds

    def dry_run(self, data):
        print("Job:", data["name"])
        for cmd in self.prep_cmds(data):
            print(cmd)
        return

    def execute(self, data, job_id, stop_event):
        """Given the job data run the command

        Note this is completely running within one thread
        Arguments:
            data {dict} -- job data
            job_id {int} -- job id that each worker keeps track of
            stop_event {threading.Event} -- an event/flag attached to each job
        """
        for cmd in self.prep_cmds(data):
            proc = subprocess.Popen(cmd)
            self._pending_procs[job_id] = proc
            proc.wait()
            self._pending_procs.pop(job_id)
            # if forcefully stopped we rely on outsider stop() to clean up
            if stop_event.is_set():
                return
        self._thread_stops.pop(job_id)
        self._threads.pop(job_id)

    def stop(self):
        """Stop jobs that are running

        Eventually we want something that can stop one specific job
        but for now we just shut down everything that's running on this worker
        """
        # set stop flags for each activa thread so that following commands
        # will not be executed
        for event in self._thread_stops.values():
            event.set()
        # terminate all currently running processes
        for proc in self._pending_procs.values():
            if proc.returncode is not None:
                proc.terminate()
        # this shouldn't do anything
        for thread in self._threads.values():
            thread.join()
        print("all threads & processes terminated!")
        self._pending_procs.clear()
        self._thread_stops.clear()
        self._threads.clear()


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
        worker.start()
    except KeyboardInterrupt:
        print('terminating...')
        worker.stop()
        exit(0)
