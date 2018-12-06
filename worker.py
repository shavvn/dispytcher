#!/usr/bin/env python3

"""
The worker process.

Each worker should be running on one and only one actual server.
It receives configurations from dispatcher and run the jobs.
So yeah, a worker is actually a server from jsonsocket...
"""

# standard library imports
import argparse
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

        # internal book keeping of running jobs
        self._job_id = 0
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
        proc_info = self.prep_proc(data)
        for cmd in proc_info['cmds']:
            proc = subprocess.Popen(
                cmd, cwd=proc_info['cwd'],
                stdout=proc_info['stdout'], stderr=proc_info['stderr'])
            self._pending_procs[job_id] = proc
            proc.wait()
            self._pending_procs.pop(job_id)
            # if forcefully stopped we rely on outside stop() to clean up
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
        stat = {"running_procs": len(self._pending_procs)}
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
    arg_parser.add_argument('hostname', type=str,
                            help='external accessible hostname of this worker')
    arg_parser.add_argument('-p', '--port', type=int, default=6666,
                            help='socket port number')
    arg_parser.add_argument('-k', '--key', type=str, default=random_key_gen(),
                            help='only work with dispatcher with matching key')

    args = arg_parser.parse_args()
    if args.port <= 1024:  # kernel port
        print("try a port # larger than 1024!")
        exit(1)
    if len(args.key) < 8:
        print("random key must have 8+ chars")
        exit(1)

    host_name = args.hostname
    worker = Worker(host_name, args.port, args.key)
    worker.start()
