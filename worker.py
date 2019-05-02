#!/usr/bin/env python3

"""
The worker process.

Each worker should be running on one and only one actual server.
It receives configurations from dispatcher and run the jobs.
So yeah, a worker is actually a server from jsonsocket...
"""

# library imports
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
import time

import docker

# local packages
from jsonsocket import Server

# optional imports
try:
    import psutil
except ImportError:
    print('psutil not installed! Functionality will be limited')
    psutil = False


class Job(object):

    def __init__(self, job_data):
        """A wrapper class for a container object

        Arguments:
            job_data {dict} -- deserialized container data received from socket
        Raised:
            ValueError -- if job name or image does not make sense
        """
        # it should be noted that name must be unique (during its life cycle)
        self.name = job_data.get('name')
        if not self.name:
            raise ValueError('Need job name!')

        self.image = job_data.get('image')
        if not self.image:
            raise ValueError('Need image name!')

        self.command = job_data.get('command', '')

        # general kwargs
        self.kwargs = job_data.get('kwargs', {})

        # each mount is a dict with 'type', 'src', 'dst', 'prop' fields
        # we need to convert them to objects
        self.mounts = job_data.get('mounts', [])
        mounts = []
        for mount_info in self.mounts:
            mount = docker.types.Mount(
                mount_info['dst'],
                mount_info['src'],
                type=mount_info['type'],
                read_only=True if mount_info['prop'] == 'ro' else False
            )
            mounts.append(mount)

        # special kwargs
        self.kwargs['name'] = self.name
        self.kwargs['mounts'] = mounts
        self.kwargs['detach'] = True

        # meta
        self.slots = job_data.get('num_slots', 1)
        self.logfile = job_data.get('logfile', '')

        # actual container object, initialized in self.run()
        self._container = None

    def run(self, docker):
        self._container = docker.containers.run(
            self.image,
            command=self.command,
            **self.kwargs
        )

    def wait(self):
        """Blocking call"""
        return self._container.wait()

    def dump_logs(self):
        if self.logfile:
            logs = self._container.logs()
            with open(self.logfile, 'wb') as fp:
                fp.write(logs)

    def stop(self):
        self._container.stop()

    def remove(self):
        self._container.remove()


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

        # docker daemon
        self.docker = docker.from_env()

        # socket server
        self.server = Server(host_ip, port)

        self._key = key

        self._total_slots = num_slots
        self._avail_slots = num_slots
        self._job_queue = []

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

        # image maintaince, image&tag as key, last updated time as value
        self._last_checked = {}

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
                logging.info('stop all jobs')
                self.stop()
            elif action == 'run':
                try:
                    job = Job(data)
                except ValueError as err:
                    logging.error('received ill-formated job info, ignoring')
                    continue

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
            elif action == 'restart':
                self.restart()
            elif action == 'debug':
                logging.debug(data)
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
        # check if there is already a running container with same name
        if job.name in self._running_jobs:
            logging.error('name conflict for job {}'.format(job.name))
            return

        # check if image up to date before running...
        self.check_update_image(job.image)

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
        try:
            job.run(self.docker)
        except docker.errors.APIError as err:
            logging.error(err)
        else:
            job.wait()
            job.dump_logs()
            job.remove()

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
                logging.info('pop queued job {} from queue')
                self.run(self._job_queue.pop(0))

    def check_update_image(self, repo, interval=60):
        """Check if image is up to date, pull the latest from registry

        By default we pull an image from registry if it hasn't been updated
        for a minute. The registry seems to return a different SHA for the
        same image from locally, so not sure if I can check an image is
        uptodate quickly. Just brute force it...

        Arguments:
            repo {str} -- repository, should include registry and tag!

        Keyword Arguments:
            interval {int} -- update intreval in seconds (default: {60})
        """

        curr_time = int(time.time())
        last_time = self._last_checked.get(repo, 0)
        if curr_time - last_time > interval:
            try:
                self.docker.images.pull(repo)
            except:
                logging.error('failed to pull image {} from registry!'
                              'Using old image if available!'.format(repo))
            else:
                logging.info('image {} pulled at {}'.format(
                    repo, curr_time))
                self._last_checked[repo] = curr_time
        else:
            logging.info('image {} updated within {}s, not pulling'.format(
                repo, interval))

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
            job.dump_logs()
            # no need to remove, already in detached mode
            job.stop()

        # this shouldn't do anything
        for thread in self._threads.values():
            thread.join()
        logging.info("all threads & processes stopped!")
        self._avail_slots = self._total_slots
        self._running_jobs.clear()
        self._thread_stops.clear()
        self._threads.clear()

    def restart(self):
        """Restart this process by starting a timed background process

        Does the following:
        - stop all running jobs on this worker
        - schedule a new worker process in 30s
        - retire this worker

        Primarily used for updating worker code, kinda hacky but works
        """
        logging.warn('restarting worker process...')
        self.stop()

        # hard code this
        config_path = os.path.join(os.environ['HOME'], '.worker.json')
        cmd = 'sleep 30 && ./worker.py {}'.format(config_path)
        subprocess.Popen(cmd, shell=True)

        self._gracefully_exit(None, None)

    def report(self):
        running_jobs = [j.name for j in self._running_jobs.values()]
        stat = {"running_jobs": running_jobs}
        stat['queued_jobs'] = [j.name for j in self._job_queue]

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
        logging.warn("gracefully shutting down...")
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


def init_logging(debug):
    logging_level = logging.INFO if not debug else logging.DEBUG
    logging.basicConfig(
        filename='{}/worker.log'.format(os.environ['HOME']),
        format='%(levelname)s|%(asctime)s: %(message)s',
        level=logging_level
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
    arg_parser.add_argument('--debug', action='store_true', default=False,
                            help='enable debug logging')

    args = arg_parser.parse_args()
    init_logging(args.debug)

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
