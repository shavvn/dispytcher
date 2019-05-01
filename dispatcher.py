#!/usr/bin/env python3

"""Job dispatcher
Send jobs in the format of JSON over the socket to the workers.
JSON files are pre-agreed format.

Ideally the situation is we use command line to specify what job we want to run
and the dispatcher automatically figures out the rest...

Things TODO:

2. automatic dispatching from the 'workers' list based on the resources needed
"""

import argparse
import json
import os
import sys
import socket
import time
from collections import OrderedDict

from jsonsocket import Client


def send_noblock(client, worker, data):
    """Send data to worker and close client afterwards

    Arguments:
        client {socket.Client} -- client socket
        worker {dict} -- provide host and port info
        data {dict} -- data to be sent in json compatible format

    Returns:
        bool -- True if send successfully, False otherwise
    """
    try:
        client.connect(worker['hostname'], worker['port'])
        # insert key into every message
        data['key'] = worker.get('key')
        client.send(data)
    except OSError as err:
        print("Cannot send to worker {}".format(worker['name']))
        print(err)
        return False
    finally:
        client.close()
    return True


def recv(client):
    data = client.recv()
    sorted_data = OrderedDict()
    for key, val in sorted(data.items()):
        sorted_data[key] = val
    return sorted_data


def send_and_recv(client, worker, data, timeout=5):
    """Send data and expect reply in timeout period

    Arguments:
        client {socket.Client} -- client socket
        worker {dict} -- provide host and port info
        data {dict} -- data to be sent in json compatible format

    Keyword Arguments:
        timeout {int} -- timeout in seconds (default: {5})

    Returns:
        dict -- send&recv status and data if received
    """
    send_status = False
    recv_status = False
    recv_data = None
    try:
        client.connect(worker['hostname'], worker['port'])
        # insert key into every message
        data['key'] = worker.get('key')
        client.send(data)
    except OSError as err:
        print("Cannot send to worker {}".format(worker['name']))
        print(err)
    else:
        send_status = True
        try:
            # TODO this timeout doesn't do shit
            # client.settimeout(timeout)
            recv_data = client.recv()
        except OSError as err:
            print("Cannot recv from worker {}".format(worker['name']))
        else:
            recv_status = True
    finally:
        client.close()
        return {
            'name': worker['name'],
            'send': send_status,
            'recv': recv_status,
            'recv_data': recv_data
        }


def get_workers(workers, worker, group):
    """Filter out actual workers to operate

    Arguments:
        workers {List} -- list of worker configurations
        worker {str} -- name of a specific worker
        group {str} -- group name of worker(s)

    Returns:
        {list} -- list of workers mapped by their name
    """
    actual_workers = []
    if worker:
        for worker_info in workers:
            if worker_info['name'] == worker:
                actual_workers.append(worker_info)
                break
    elif group:
        for worker_info in workers:
            if group in worker_info['groups']:
                actual_workers.append(worker_info)
    else:
        for worker_info in workers:
            actual_workers.append(worker_info)
    return actual_workers


def get_jobs(jobs, job, tag):
    """Filter out actual jobs to run

    The config file should provide a full list of jobs, and the user
    specify job/tag to select among them.

    Arguments:
        jobs {List} -- list of dicts of all possible jobs
        job {str} -- name of a specific job
        tag {str} -- tag of job/jobs

    Returns:
        {list} -- list of jobs mapped by their name
    """
    actual_jobs = []
    if job:
        for job_data in jobs:
            if job_data['name'] == job:
                actual_jobs.append(job_data)
                break
    elif tag:
        for job_data in jobs:
            if tag in job_data['tags']:
                actual_jobs.append(job_data)
    else:
        for job_data in jobs:
            actual_jobs.append(job_data)

    if not actual_jobs:
        print('Did not find matching jobs!')
        exit(1)
    return actual_jobs


def run(workers, jobs, debug=False):
    total_slots = sum([w['num_slots'] for w in workers])
    if total_slots < len(jobs):
        print('Warning! More jobs than workers can handle!',
              'Jobs that cannot be sent will be dumped as remaining_jobs.json')
        remaining_jobs = jobs[total_slots:]
    else:
        remaining_jobs = []

    # we do round robin assignment of jobs to workers
    assignment = {}
    i, j = 0, 0
    while i < min(len(jobs), total_slots):
        if j == len(workers):
            j = 0
        worker = workers[j]
        if worker['num_slots'] > 0:
            assignment[i] = j
            worker['num_slots'] -= 1
            j += 1
            i += 1
        else:
            j += 1

    for i, j in assignment.items():
        print("Assigning {} to {}".format(jobs[i]['name'],
                                          workers[j]['name']))

    for job in jobs:
        job['action'] = 'run'

    client = Client()
    sent_jobs = set()
    for i, j in assignment.items():
        job = jobs[i]
        worker = workers[j]
        if not debug:
            if not send_noblock(client, worker, job):
                remaining_jobs.append(job)
            else:
                sent_jobs.add(job['name'])
                print('Sent job {} to {}'.format(job['name'], worker['name']))
        else:
            print('Sending job {} to {}'.format(job['name'], worker['name']))
        time.sleep(0.1)

    if len(remaining_jobs) > 0:
        if os.path.exists('remaining_jobs.json'):
            with open('remaining_jobs.json', 'r') as fp:
                prev_jobs = json.load(fp)
                for job in prev_jobs:
                    if job['name'] not in sent_jobs:
                        remaining_jobs.append(job)

        with open('remaining_jobs.json', 'w') as fp:
            json.dump(remaining_jobs, fp, indent=4)
    return


def report(workers):
    client = Client()
    for worker in workers:
        data = send_and_recv(client, worker, {'action': 'report'})
        print(data)


def broadcast(workers, message):
    """Broadcasting message to workers

    Expect no reply, skip when encounters failure
    Arguments:
        workers {list} -- list of workers to be broadcasted to
        message {obj} -- json compatible data structures
    """
    client = Client()
    for worker in workers:
        print("Broadcasting to {}".format(worker['name']))
        send_noblock(client, worker, message)
    return


def load_json(config_file):
    with open(config_file, 'r') as fp:
        data = json.load(fp)
    return data


def add_worker_options(parser):
    parser.add_argument('-u', '--union', type=str,
                        help='json file that has list of all available workers'
                        '. Default value supplied to save typing',
                        default='.last_workers.json')
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
    restart_cmd = subparsers.add_parser(
        'restart', help='restart worker'
    )
    add_job_options(run_cmd)
    add_job_options(stop_cmd)
    for cmd in [run_cmd, stop_cmd, report_cmd, retire_cmd, restart_cmd]:
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

    union_data = load_json(args.union)
    workers = get_workers(union_data['workers'], args.worker, args.group)

    if args.sub_cmd == 'run':
        listing_data = load_json(args.listings)
        jobs = get_jobs(listing_data, args.job, args.tag)
        run(workers, jobs)
    elif args.sub_cmd == 'report':
        report(workers)
    elif args.sub_cmd in set(['stop', 'retire', 'restart']):
        broadcast(workers, {'action': args.sub_cmd})
    else:
        print("Unsupported action!")
        exit(1)
