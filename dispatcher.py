#!/usr/bin/env python3

"""Job dispatcher
Send jobs in the format of JSON over the socket to the workers.
JSON files are pre-agreed format.

Ideally the situation is we use command line to specify what job we want to run
and the dispatcher automatically figures out the rest...

Things TODO:

2. automatic dispatching from the 'workers' list based on the resources needed
3. command line to json conversion
4. should probably make use of regristry for lookup
    - should it be local or remote or both? (thinking remote now..)
"""

import argparse
import json
import os
import sys
import socket
from collections import OrderedDict

from jsonsocket import Client


class Dispatcher(object):

    def __init__(self, union_config, listing_config, job=None,
                 tag=None, worker=None, group=None):
        """Create a one-time dispatcher sending jobs/actions out

        config contains all of job/worker info so that job/tag/worker/group
        can be just short names.
        job/tag sepcify which job/jobs to run, worker/group specify
        worker/workers to operate.
        different actions leads to different commands sent to workers

        Arguments:
            config {dict} -- config dict that has worker and job info

        Keyword Arguments:
            job {str} -- job name (default: {None})
            tag {str} -- tag associates with multiple jobs (default: {None})
            worker {str} -- worker name (default: {None})
            group {str} -- worker group that define multiple workers
                           (default: {None})
        """

        if not (job or tag):
            print('No job or tag specified, running all jobs!')
        if job and tag:
            print('Specify only job or tag, not both')
            exit(1)

        # sender
        self.client = Client()

        self.job_mapping = {}

        # figure out actual jobs or workers
        self.workers = self.get_workers(config['workers'], worker, group)
        self.jobs = self.get_jobs(config['jobs'], job, tag)

    def get_workers(self, workers, worker, group):
        """Filter out actual workers to operate

        Arguments:
            workers {List} -- list of worker configurations
            worker {str} -- name of a specific worker
            group {str} -- group name of worker(s)

        Returns:
            {dict} -- list of workers mapped by their name
        """
        actual_workers = {}
        if worker:
            for worker_info in workers:
                if worker_info['name'] == worker:
                    actual_workers[worker_info['name']] = worker_info
                    break
        elif group:
            for worker_info in workers:
                if group in worker_info['groups']:
                    actual_workers[worker_info['name']] = worker_info
        else:
            for worker_info in workers:
                actual_workers[worker_info['name']] = worker_info
        return actual_workers

    def get_jobs(self, jobs, job, tag):
        """Filter out actual jobs to run

        The config file should provide a full list of jobs, and the user
        specify job/tag to select among them.

        Arguments:
            jobs {List} -- list of dicts of all possible jobs
            job {str} -- name of a specific job
            tag {str} -- tag of job/jobs

        Returns:
            {dict} -- list of jobs mapped by their name
        """
        actual_jobs = {}
        if job:
            for job_data in jobs:
                if job_data['name'] == job:
                    actual_jobs[job_data['name']] = job_data
                    break
        elif tag:
            for job_data in jobs:
                if tag in job_data['tags']:
                    actual_jobs[job_data['name']] = job_data
        else:
            for job_data in jobs:
                actual_jobs[job_data['name']] = job_data

        if not actual_jobs:
            print('Did not find matching jobs!')
            exit(1)
        return actual_jobs

    def run(self, action):
        if action == 'run' or action == 'dry':
            self.dispatch(action)
        elif action == 'report':
            self.report()
        elif action == 'stop':
            self.send_stop()
        elif action == 'retire':
            self.send_retire()
        self.close()

    def dispatch(self, action):
        """Dispatching jobs to workers

        Given the slots available and slots needed, send jobs to workers.

        TODO test which workers are actually online and how busy they are
        TODO save jobs that are not sent out successfully for a resend later
        Raises:
            Exception -- if not enough worker slots raise this exception
        """

        worker_itr = iter(self.workers.values())
        worker = next(worker_itr)
        for job_name, job in self.jobs.items():
            if job['num_slots'] <= worker['num_slots']:
                self.job_mapping[job_name] = worker
                worker['num_slots'] -= job['num_slots']
            else:
                worker = next(worker_itr)

        if len(self.job_mapping) < len(self.jobs):
            raise Exception('Not enough resources for all jobs!')

        for job_name, worker in self.job_mapping.items():
            self.jobs[job_name]['action'] = action
            self._send(worker, self.jobs[job_name])
            print('sent job {} to {}'.format(job_name, worker['name']))
            self.close()

    def report(self):
        for worker in self.workers.values():
            if not self._send(worker, {'action': 'report'}):
                continue
            while True:
                try:
                    data = self._recv()
                except (ValueError, OSError) as e:
                    print("No response from {}".format(worker['name']))
                    print(e)
                    continue
                except Exception as e:
                    print('Unexpected error!')
                    print(e)
                    continue
                print('Worker "{}":'.format(worker['name']))
                for key, val in data.items():
                    print('    {}: {}'.format(key, val))
                self.close()
                break

    def send_stop(self):
        for worker in self.workers.values():
            print("Sending stop to {}".format(worker['name']))
            self._send(worker, {'action': 'stop'})
            self.close()

    def send_retire(self):
        for worker in self.workers.values():
            print("Retiring worker {}".format(worker['name']))
            self._send(worker, {'action': 'retire'})
            self.close()

    def _send(self, worker, data):
        try:
            self.client.connect(worker['hostname'], worker['port'])
            # insert key into every message
            data['key'] = worker.get('key')
            self.client.send(data)
        except OSError as err:
            print("Cannot send to worker {}".format(worker['name']))
            print(err)
            return False
        return True

    def _recv(self):
        data = self.client.recv()
        sorted_data = OrderedDict()
        for key, val in sorted(data.items()):
            sorted_data[key] = val
        return sorted_data

    def close(self):
        self.client.close()


def load_config(config_file):
    """Loading config json file and return a dict

    Also do some basic sanity check here

    Arguments:
        config_file {str} -- config json file

    Returns:
        dict -- dict of that json
    """

    with open(config_file, 'r') as fp:
        data = json.load(fp)
    return data


def add_worker_options(parser):
    parser.add_argument('-u', '--union', type=str,
                        help='json file that has list of all available workers'
                        '. Default value supplied to save typing',
                        default='workers.json')
    parser.add_argument('-w', '--worker', type=str,
                        help='specific worker name')
    parser.add_argument('-g', '--group', type=str,
                        help='specific worker group name')
    return parser


def add_job_options(parser):
    parser.add_argument('-l', '--listings', type=str,
                        help='json file that has list of all available jobs'
                        '. Default value supplied to save typing',
                        default='jobs.json')
    parser.add_argument('-j', '--job', type=str, help='specifc job name')
    parser.add_argument('-t', '--tag', type=str, help='specifc job tag')
    return parser


def init_argparser():
    # main parser
    parser = argparse.ArgumentParser(description='Dispatcher')
    subparsers = parser.add_subparsers(
        dest='sub_cmd', title='sub-commands', help='sub-command help')

    # sub command parsers
    run_cmd = subparsers.add_parser('run', help='run one or more jobs')
    stop_cmd = subparsers.add_parser('stop', help='stop one or more jobs')
    retire_cmd = subparsers.add_parser(
        'retire', help='retire worker for good')
    report_cmd = subparsers.add_parser(
        'report', help='report worker status (jobs, CPU, mem)')
    add_job_options(run_cmd)
    add_job_options(stop_cmd)
    for cmd in [run_cmd, stop_cmd, report_cmd, retire_cmd]:
        add_worker_options(cmd)

    return parser


def sanitize_args(args):
    if args.sub_cmd == 'run' or args.sub_cmd == 'stop':
        if not args.listings:
            print('"run" or "stop" command need job listings', file=sys.stderr)
            exit(1)


if __name__ == '__main__':
    arg_parser = init_argparser()
    args = arg_parser.parse_args()
    if not args.sub_cmd:
        print("Sub-command required!", file=sys.stderr)
        arg_parser.print_help()

    print(args)
    union_config = load_config(args.union)
    listing_config = load_config(args.listings)

    # hmmm it's stateless anyway why do I need an object..
    dispatcher = Dispatcher(union_config, job=args.job,
                            tag=args.tag, worker=args.worker)

    # dispatcher = Dispatcher(config, job=args.job, tag=args.tag,
    #                         worker=args.worker, group=args.group)
    # dispatcher.run(action)
    # dispatcher.close()
    # exit(0)
