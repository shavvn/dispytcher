import unittest
import dispatcher
import worker
import jsonsocket


class TestWokerUtil(unittest.TestCase):

    def setUp(self):
        self.worker_info = {
            'hostname': 'localhost',
            'port': 6666,
            'key': 'deadbeef',
            'num_slots': 4
        }
        self.client = jsonsocket.Client()
        self.worker = worker.Worker(
            self.worker_info['hostname'],
            self.worker_info['port'],
            self.worker_info['key'],
            self.worker_info['num_slots']
        )

    def test_init_server_local(self):
        try:
            server = worker.Worker('localhost', 6667, 'deadbeef', 1)
        except OSError as err:
            self.fail('cannot create server on local host')

        # giving a nonesense host name and expect an exception
        with self.assertRaises(OSError):
            worker.Worker('blah', 1234, 'deadbeaf', 1)

    def test_run_and_queue(self):
        # The worker.start() will block, will have to use multi threads
        # to test this, oh well..
        self.assertEqual(self.worker._avail_slots, 4)
        self.assertEqual(len(self.worker._job_queue), 0)


if __name__ == "__main__":
    unittest.main()
