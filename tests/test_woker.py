import unittest
import worker


class TestWokerUtil(unittest.TestCase):

    def test_init_server_local(self):
        try:
            server = worker.init_server('localhost', 3333)
            server.close()
        except ExceptionType:
            self.fail('cannot even create server on local host')

        # giving a nonesense host name and expect an exception
        with self.assertRaises(OSError):
            worker.init_server('blah', 1234)
