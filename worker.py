#!/usr/bin/env python3

"""
The worker process.

Each worker should be running on one and only one actual server.
It receives configurations from dispatcher and run the jobs.
So yeah, a worker is actually a server from jsonsocket...
"""

# standard library imports
import argparse
import json
import os
import random
import signal
import socket
import string
import subprocess
import threading

# thrid party imports
try:
    import psutil
except ImportError:
    print('psutil not installed! Functionality will be limited')
    psutil = False

# local packages
from jsonsocket import Server


class ProcWrapper(object):

    def __init__(self, job_data):
        """A Wrapper around Popen object for ease management

        Arguments:
            job_data {dict} -- deserialized job data received from socket
        Raised:
            ValueError -- if job name or cmds does not make sense
        """
        # it should be noted that name is not unique
        self.name = job_data.get('name')
        if not self.name:
            raise ValueError("Job name not received!")

        self.cmds = job_data.get('cmds')
        if not self.cmds or (not isinstance(self.cmds, list)):
            raise ValueError("Job cmds not correct!")

        self.cwd = job_data.get('cwd', '.')
        if not os.path.exists(self.cwd):
            try:
                os.mkdir(self.cwd)
            except:
                print("Invalid CWD!")
                self.cwd = '.'

        self.stdout = None
        stdout_name = job_data.get('stdout')
        if stdout_name:
            try:
                self.stdout = open(stdout_name, 'w')
            except Exception as e:
                print("cannot use {} for stdout".format(stdout_name))
        self.stderr = self.stdout
        stderr_name = job_data.get('stderr')
        if stderr_name and stderr_name != stdout_name:
            try:
                self.stderr = open(stderr_name, 'w')
            except Exception as e:
                print("cannot use {} for stderr".format(stdout_name))

        # actual Popen object
        self._proc = None

    def run(self, cmd):
        self._proc = subprocess.Popen(
            cmd, cwd=self.cwd,
            stdout=self.stdout, stderr=self.stderr)
        return self

    def wait(self):
        """Blocking call"""
        return self._proc.wait()

    def poll(self):
        return self._proc.poll()

    def terminate(self):
        return self._proc.terminate()


class Worker(object):

    def __init__(self, host_name, port, key):
        """Initialize a server process

        Arguments:
            host_name {string} -- [host ip]
            port {int} -- [socket port]

        Raises:
            OSError -- happens when resolving host or creating socket
        """
        host_ip = socket.gethostbyname(host_name)
        self.server = Server(host_ip, port)
        self._key = key

        # unique id for each job received
        self._job_id = 0

        # because it's highly likely to receive multiple jobs at once
        # and each job can take very long, we don't want to block following
        # jobs, so we use one thread for each job received and use these
        # data structures to keep track of threads, indexed by _job_id
        self._threads = {}
        self._thread_stops = {}
        self._pending_procs = {}

        # signal handler to kill the process
        signal.signal(signal.SIGINT, self._gracefully_exit)
        signal.signal(signal.SIGTERM, self._gracefully_exit)

    def start(self):
        while True:
            self.server.accept()  # blocking
            try:
                data = self._recv()
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
            elif action == 'report':
                self.report()
            elif action == 'retire':
                self._gracefully_exit(None, None)
            else:
                continue
        return True

    def prep_proc(self, data):
        """Prepare the data a subprocess needs

        Currently there are 3 fields:
        cmd: a list of program + args that can be put into a subprocess call.
        stdout: standard output file handler
        stderr: standard error file handler

        Arguments:
            data {dict} -- job data

        Returns:
            dict -- key value pairs of the above mentioned fields
        """
        working_dir = data.get('cwd', '.')
        stdout = None
        stdout_name = data.get('stdout')
        if stdout_name:
            try:
                stdout = open(stdout_name, 'w')
            except Exception as e:
                print("cannot use {} for stdout".format(stdout_name))
                pass
        stderr_name = data.get('stderr')
        stderr = stdout
        if stderr_name and stderr_name != stdout_name:
            try:
                stderr = open(stderr_name, 'w')
            except Exception as e:
                print("cannot use {} for stderr".format(stdout_name))
                pass
        return {'cwd': working_dir, 'cmds': data['cmds'],
                'stdout': stdout, 'stderr': stderr}

    def dry_run(self, data):
        print("Job:", data['name'])
        print(self.prep_proc(data))
        return

    def execute(self, data, job_id, stop_event):
        """Given the job data run the command

        Note this is completely running within one thread
        Arguments:
            data {dict} -- job data
            job_id {int} -- job id that each worker keeps track of
            stop_event {threading.Event} -- an event/flag attached to each job
        """
        proc = ProcWrapper(data)
        self._pending_procs[job_id] = proc
        for cmd in proc.cmds:
            proc.run(cmd)
            proc.wait()
            # if receiving stop signal, do not run subsequent cmds
            if stop_event.is_set():
                return
        # normal finishing procedures
        self._pending_procs.pop(job_id)
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
            if proc.poll() is None:
                proc.terminate()
        # this shouldn't do anything
        for thread in self._threads.values():
            thread.join()
        print("all threads & processes terminated!")
        self._pending_procs.clear()
        self._thread_stops.clear()
        self._threads.clear()

    def report(self):
        stat = {"running_procs": [p.name
                                  for p in self._pending_procs.values()]}
        if psutil:
            mem = psutil.virtual_memory()
            mega = 1024 * 1024
            stat['mem_total(MB)'] = round(mem.total / mega)
            stat['mem_available(MB)'] = round(mem.available / mega)
            stat['cpu_usage(%)'] = psutil.cpu_percent()
            stat['mem_usage(%)'] = mem.percent
        try:
            self.server.settimeout(3.0)
            self.server.send(stat)
            self.server.settimeout(None)
        except (ValueError, OSError) as e:
            print("cannot send report, continue operation")
            print(e.message)
        print("stats sent out")
        return

    def _recv(self):
        """Customized receive function

        Returns:
            dict -- empty if key not match
        """
        data = self.server.recv()
        key = data.get('key')
        if key != self._key:
            print("key does not match!, ignore message")
            return {}
        else:
            return data

    def _gracefully_exit(self, signum, frame):
        print("Gracefully shutting down...")
        self.stop()
        self.server.close()
        exit(0)


def random_key_gen(n=8):
    """Generate a random key string

    Keyword Arguments:
        n {int} -- key length in chars, at least 8 (default: {8})

    Returns:
        str -- random string
    """
    if n < 8:
        raise ValueError("random key must has 8+ chars")
    chars = string.ascii_letters + string.digits
    # random.choices only works with python3.6+
    return ''.join(random.choice(chars) for _ in range(n))


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser(description='Initialize worker')
    arg_parser.add_argument('-c', '--config', type=str,
                            help='config json file contains host info.\n'
                            'Must have following fields: hostname, port')
    arg_parser.add_argument('-H', '--hostname', type=str,
                            help='external accessible hostname of this worker')
    arg_parser.add_argument('-p', '--port', type=int, default=6666,
                            help='socket port number')
    arg_parser.add_argument('-k', '--key', type=str,
                            help='only work with dispatcher with matching key')

    args = arg_parser.parse_args()

    hostname = None
    port = None
    key = None
    if args.hostname and not args.config:
        hostname = args.hostname
        print("")
        if args.port <= 1024:  # kernel port
            print("try a port # larger than 1024!")
            exit(1)
        port = args.port
    elif args.config and not args.hostname:
        with open(args.config, 'r') as fp:
            config = json.load(fp)
            hostname = config['hostname']
            port = config['port']
            if config.get('key'):  # may want key be secret
                key = config['key']
        pass
    else:
        print("please specify either hostname or config file")
        exit(1)

    if not key:
        if not args.key:
            key = random_key_gen(12)
        elif len(args.key) < 8:
            print("key must have 8+ chars")
            exit(1)
        else:
            key = args.key
    worker = Worker(hostname, port, key)
    worker.start()
