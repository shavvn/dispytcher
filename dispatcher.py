#!/usr/bin/env python3

"""Job dispatcher
Send jobs in the format of JSON over the socket to the workers.
JSON files are pre-agreed format.

Ideally the situation is we use command line to specify what job we want to run
and the dispatcher automatically figures out the rest...

Things TODO:

1. define the json format that server and client use to communicate about jobs
2. automatic dispatching from the 'workers' list based on the resources needed
3. command line to json conversion
4. should probably make use of regristry for lookup
    - should it be local or remote or both? (thinking remote now..)
5. assuming all workers have clean state or dynimically track how many slots?
"""

import argparse
import json
import os

from job import Job
from jsonsocket import Client


class Dispatcher(object):

    def __init__(self, config, job=None, tag=None, worker=None):
        if not (job or tag):
            print('No job or tag specified, running all jobs!')
        if job and tag:
            print('Specify only job or tag, not both')
            exit(1)

        if not worker:
            print('No worker specified, automatically choosing')
        self.client = Client()

        job_file = config['jobs']
        with open(job_file, 'r') as fp:
            jobs = json.load(fp)

        # figure out actual jobs to run
        self.jobs = {}
        if job:
            for job_data in jobs:
                if job_data['name'] == job:
                    self.jobs[job_data['name']] = job_data
                    break
        elif tag:
            for job_data in jobs:
                if tag in job_data['tags']:
                    self.jobs[job_data['name']] = job_data
        else:
            for job_data in jobs:
                self.jobs[job_data['name']] = job_data

        if not self.jobs:
            print('Did not find matching jobs!')
            exit(1)

        self.workers = config['workers']
        self.actiave_workers = []
        self.job_mapping = {}

    def dispatch(self):
        """Dispatching jobs to workers

        Given the slots available and slots needed, send jobs to workers.

        TODO test which workers are actually online and how busy they are
        Raises:
            Exception -- if not enough worker slots raise this exception
        """

        i = 0
        worker = self.workers[i]
        for job_name, job in self.jobs.items():
            if job['num_slots'] <= worker['num_slots']:
                self.job_mapping[job_name] = worker
                worker['num_slots'] -= job['num_slots']
            else:
                i += 1
                worker = self.workers[i]

        if len(self.job_mapping) < len(self.jobs):
            raise Exception('Not enough resources for all jobs!')

        for job_name, worker in self.job_mapping.items():
            self._send(worker, self.jobs[job_name])

    def _send(self, worker, job):
        print('Sending {} to {}'.format(job['name'], worker['name']))
        self.client.connect(worker['hostname'], worker['port'])
        self.client.send(job)
        self.client.close()

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
    # basic sanity check
    if 'workers' not in data or len(data['workers']) == 0:
        print('No workers in config file!')
        exit(1)

    if 'jobs' not in data:
        print('No jobs in config file!')

    jobs_file = data['jobs']
    if not os.path.exists(jobs_file):
        print('cannot locate jobs file in config!')
        exit(1)
    return data


def init_argparser():
    parser = argparse.ArgumentParser(description='Dispatcher process')
    parser.add_argument('config', type=str, help='configuration file')
    parser.add_argument('-j', '--job', type=str, help='specifc job name')
    parser.add_argument('-t', '--tag', type=str, help='specifc job tag')
    parser.add_argument('-w', '--worker', type=str,
                        help='specific worker name')
    return parser


if __name__ == '__main__':
    arg_parser = init_argparser()
    args = arg_parser.parse_args()

    config = load_config(args.config)
    dispatcher = Dispatcher(config, args.job, args.tag, args.worker)
    dispatcher.dispatch()
    dispatcher.close()
    exit(0)