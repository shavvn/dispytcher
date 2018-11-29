import unittest
import worker


class TestWokerUtil(unittest.TestCase):

    def test_init_server_local(self):
        try:
            server = worker.Worker('localhost', 6666)
            server.close()
        except ExceptionType:
            self.fail('cannot create server on local host')

        # giving a nonesense host name and expect an exception
        with self.assertRaises(OSError):
            worker.Worker('blah', 1234)
