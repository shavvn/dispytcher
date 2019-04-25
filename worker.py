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
import logging
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

    def __init__(self, job_data):
        """A Job class for ease management

        A job can have multiple commands, but only ONE cwd, stdout, stderr.
        All the commands should be executed in order, one by one.

        Arguments:
            job_data {dict} -- deserialized job data received from socket
            job_id {int} -- unique id assigned by worker
        Raised:
            ValueError -- if job name or cmds does not make sense
        """
        # it should be noted that name must be unique (during its life cycle)
        self.name = job_data.get('name')
        if not self.name:
            raise ValueError("Job name not received!")

        self.cmds = job_data.get('cmds')
        if not self.cmds or (not isinstance(self.cmds, list)):
            raise ValueError("Job cmds format not correct!")

        self.slots = job_data.get('num_slots')

        self.cwd = job_data.get('cwd', '.')
        if not os.path.exists(self.cwd):
            try:
                os.mkdir(self.cwd)
            except:
                logging.warn("Invalid CWD! {}".format(self.cwd))
                self.cwd = '.'

        # actual Popen object
        self._proc = None

    def run(self, cmd):
        """
        TODO you can use docker -H to get around this kind of hack
        Or... you can use docker-py PDK
        """
        self._proc = subprocess.Popen(cmd, cwd=self.cwd)
        return self

    def wait(self):
        """Blocking call"""
        return self._proc.wait()

    def poll(self):
        return self._proc.poll()

    def terminate(self):
        return self._proc.terminate()


class Worker(object):

    def __init__(self, host_name, port, key, num_slots):
        """Initialize a server process

        Arguments:
            host_name {string} -- host ip
            port {int} -- socket port
            num_slots {int} -- total number of slots for this worker

        Raises:
            OSError -- happens when resolving host or creating socket
        """
        host_ip = socket.gethostbyname(host_name)
        self.server = Server(host_ip, port)
        self._key = key

        self._total_slots = num_slots
        self._avail_slots = num_slots
        self._job_queue = []

        # unique id for each job received
        # TODO should just use job name
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
                logging.error('Cannot recv data! Closing socket...')
                logging.error(e.message)
                # forcing client to close to free up resource
                continue
            except Exception as e:
                logging.error('Unexpected error!')
                logging.error(e.message)
                continue

            action = data.get('action')
            if action == 'stop':
                # TODO cannot stop a docker container
                # need to save container name and use docker utility to stop
                logging.info('stop all jobs')
                self.stop()
            elif action == 'run':
                job = Job(data)
                logging.info('receving job {}'.format(job.name))
                if job.slots > self._avail_slots:
                    logging.info('no enough slots, {} queued'.format(job.name))
                    self._job_queue.append(job)
                else:
                    self.run(job)
            elif action == 'report':
                self.report()
            elif action == 'retire':
                self._gracefully_exit(None, None)
            elif action == 'debug':
                print(data)
            else:
                continue

        return True

    def run(self, job):
        """Create a thread to run a job

        We need a thread because we don't want to block following jobs
        execute() is the thread target function

        Arguments:
            job {Job} -- Job object
        """
        self._avail_slots -= job.slots
        logging.info(
            'start running {}, avail slots {}/{}'.format(
                job.name, self._avail_slots, self._total_slots))
        stop_event = threading.Event()
        thread = threading.Thread(
            target=self.execute,
            args=(job, stop_event,)
        )
        self._thread_stops[job.name] = stop_event
        self._threads[job.name] = thread
        thread.start()
        return

    def execute(self, job, stop_event):
        """Given the job data run the command

        Note this is completely running within one thread
        Arguments:
            job {Job} -- job object
            stop_event {threading.Event} -- an event/flag attached to each job
        """
        self._running_jobs[job.name] = job
        for cmd in job.cmds:
            job.run(cmd)
            job.wait()
            # if receiving stop signal, do not run subsequent cmds
            if stop_event.is_set():
                return
        # normal finishing procedures
        self._avail_slots += job.slots
        self._running_jobs.pop(job.name)
        self._thread_stops.pop(job.name)
        self._threads.pop(job.name)
        logging.info('job {} done'.format(job.name))

        # check if there's queued job, strictly FIFO?
        if len(self._job_queue) > 0:
            if self._job_queue[0].slots <= self._avail_slots:
                logging.info('pop queued job from queue')
                self.run(self._job_queue.pop(0))

    def stop(self):
        """Stop all jobs that are running and clear the ones are queued

        Eventually we want something that can stop one specific job
        but for now we just shut down everything that's running on this worker
        """
        # clean job queue
        self._job_queue.clear()

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
        logging.info("all threads & processes stopped!")
        self._avail_slots = self._total_slots
        self._running_jobs.clear()
        self._thread_stops.clear()
        self._threads.clear()

    def show_output(self, job_data):
        """TODO use docker logs maybe

        Arguments:
            job_data {[type]} -- [description]
        """
        return

    def report(self):
        running_jobs = [j.name for j in self._running_jobs.values()]
        stat = {"running_jobs": running_jobs}
        stat['queued_jobs'] = [j['name'] for j in self._job_queue]

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
            logging.warn("cannot send report, continue operation")
            logging.warn(e.message)
        logging.info("stats sent out")
        return

    def _recv(self):
        """Customized receive function

        Returns:
            dict -- empty if key not match
        """
        data = self.server.recv()
        key = data.get('key')
        if key != self._key:
            logging.warn("key does not match!, ignore message")
            return {}
        else:
            return data

    def _gracefully_exit(self, signum, frame):
        logging.warn("Gracefully shutting down...")
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


def init_logging():
    logging.basicConfig(
        filename='{}/worker.log'.format(os.environ['HOME']),
        format='%(levelname)s|%(asctime)s: %(message)s',
        level=logging.DEBUG
    )
    return


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser(description='Initialize worker')
    arg_parser.add_argument('config', type=str, default=None,
                            help='config json file contains host info.\n'
                            'Specify: hostname, port, num_slots, [key]')
    arg_parser.add_argument('-H', '--hostname', type=str,
                            help='external accessible hostname of this worker')
    arg_parser.add_argument('-p', '--port', type=int, default=6666,
                            help='socket port number')
    arg_parser.add_argument('-s', '--slots', default=8,
                            help='total slots for this worker')
    arg_parser.add_argument('-k', '--key', type=str, default=None,
                            help='only work with dispatcher with matching key')

    args = arg_parser.parse_args()
    init_logging()

    hostname = None
    port = None
    slots = 0
    key = None
    if args.config is None:
        hostname = args.hostname
        if args.port <= 1024:  # kernel port
            logging.error("try a port # larger than 1024!")
            exit(1)
        port = args.port
        slots = args.slots
        key = args.key
    else:
        with open(args.config, 'r') as fp:
            config = json.load(fp)
            hostname = config['hostname']
            port = config['port']
            slots = config['num_slots']
            if config.get('key'):  # may want key be secret
                key = config['key']

    if not key:
        if not args.key:
            key = random_key_gen(12)
        elif len(args.key) < 8:
            logging.error("key must have 8+ chars")
            exit(1)
        else:
            key = args.key
    worker = Worker(hostname, port, key, slots)
    worker.start()
