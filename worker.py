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


class Job(object):

    def __init__(self, job_data, job_id):
        """A Job class for ease management

        A job can have multiple commands, but only ONE cwd, stdout, stderr.
        All the commands should be executed in order, one by one.

        Arguments:
            job_data {dict} -- deserialized job data received from socket
            job_id {int} -- unique id assigned by worker
        Raised:
            ValueError -- if job name or cmds does not make sense
        """
        # it should be noted that name may not be unique
        self.name = job_data.get('name')
        if not self.name:
            raise ValueError("Job name not received!")
        self.id = job_id

        self.cmds = job_data.get('cmds')
        if not self.cmds or (not isinstance(self.cmds, list)):
            raise ValueError("Job cmds format not correct!")

        self.cwd = job_data.get('cwd', '.')
        if not os.path.exists(self.cwd):
            try:
                os.mkdir(self.cwd)
            except:
                print("Invalid CWD! {}".format(self.cwd))
                self.cwd = '.'

        self.stdout = None
        stdout_name = job_data.get('stdout')
        if stdout_name:
            try:
                self.stdout = open(stdout_name, 'w')
            except Exception as e:
                print("cannot use {} for stdout".format(stdout_name))
                print(e)

        self.stderr = self.stdout
        stderr_name = job_data.get('stderr')
        if stderr_name and stderr_name != stdout_name:
            try:
                self.stderr = open(stderr_name, 'w')
            except Exception as e:
                print("cannot use {} for stderr".format(stdout_name))
                print(e)

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

    def flush(self):
        """Flush stdout & stderr buffers"""
        if self.stdout:
            self.stdout.flush()
        if self.stderr:
            self.stderr.flush()


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
        # we use {} instead of [] as they are more robust for multi-threads
        self._threads = {}
        self._thread_stops = {}
        self._running_jobs = {}

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
                self._job_id += 1
                thread.start()
            elif action == 'report':
                self.report()
            elif action == 'retire':
                self._gracefully_exit(None, None)
            else:
                continue
        return True

    def dry_run(self, data):
        print("Job:", data['name'])
        print(data['cmds'])
        return

    def execute(self, data, job_id, stop_event):
        """Given the job data run the command

        Note this is completely running within one thread
        Arguments:
            data {dict} -- job data
            job_id {int} -- job id that each worker keeps track of
            stop_event {threading.Event} -- an event/flag attached to each job
        """
        job = Job(data, job_id)
        self._running_jobs[job_id] = job
        for cmd in job.cmds:
            job.run(cmd)
            job.wait()
            # if receiving stop signal, do not run subsequent cmds
            if stop_event.is_set():
                return
        # normal finishing procedures
        self._running_jobs.pop(job_id)
        self._thread_stops.pop(job_id)
        self._threads.pop(job_id)

    def stop(self):
        """Stop all jobs that are running

        Eventually we want something that can stop one specific job
        but for now we just shut down everything that's running on this worker
        """
        # set stop flags for each activa thread so that following commands
        # will not be executed
        for event in self._thread_stops.values():
            event.set()
        # terminate all currently running processes
        for job in self._running_jobs.values():
            if job.poll() is None:
                job.terminate()
        # this shouldn't do anything
        for thread in self._threads.values():
            thread.join()
        print("all threads & processes terminated!")
        self._running_jobs.clear()
        self._thread_stops.clear()
        self._threads.clear()

    def show_output(self, job_data):
        """Show stdout and stderr of a job

        The stdout and stderr will be flushed, and the contents will be
        sent to dispatcher as text/str over socket
        Only send the last 20 lines of output

        Arguments:
            job_name {dict} -- job data
        """
        job = None
        send_lines = None
        for j in self._running_jobs.values():
            if j.name == job_name:
                job = j
        if job:
            job.flush()
            with open(job.stdout.name, 'r') as fp:
                lines = fp.readlines()
                if len(lines) > 20:
                    send_lines = lines[-20:]
                else:
                    send_lines = lines
        else:
            with open(job_data['stdout'], 'r') as fp:
                lines = fp.readlines()
                if len(lines) > 20:
                    send_lines = lines[-20:]
                else:
                    send_lines = lines
        if send_lines:
            self.server.send(send_lines)

    def report(self):
        running_jobs = [j.name for j in self._running_jobs.values()]
        stat = {"running_jobs": running_jobs}

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
