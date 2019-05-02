import random
import string
import unittest
import contextlib
import time
from redis import ResponseError

from redis_server import RedisServer

LOCAL_GID = 2

current_milli_time = lambda: int(round(time.time() * 1000))


def random_str(length):
    return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(length))


class BaseModuleTestCase(unittest.TestCase):

    """
    You can inherit from this base class directly. The server, port, and module
    settings can be defined either directly via the config module (see the
    config.py file), or via the rmtest.config file in the current directoy (i.e.
    of the process, not the file), or via environment variables.
    """

    def setUp(self):
        super(BaseModuleTestCase, self).setUp()
        self._ensure_server()

    def tearDown(self):
        if hasattr(self, '_server'):
            self._server.stop()
            self._server = None
            self._client = None

        super(BaseModuleTestCase, self).tearDown()

    @property
    def server(self):
        self._ensure_server()
        return self._server

    @property
    def client(self):
        self._ensure_server()
        return self._client

    def _ensure_server(self):
        if getattr(self, '_server', None):
            return
        self._server = RedisServer(module_args=self.module_args)
        self._server.start()
        self._client = self._server.client()

    @property
    def module_args(self):
        """
        Module-specific arguments required
        """
        return ['CRDT.GID', '2']

    def cmd(self, *args, **kwargs):
        return self.client.execute_command(*args, **kwargs)

    def assertOk(self, x, msg=None):
        if type(x) == type(b""):
            self.assertEqual(b"OK", x, msg)
        else:
            self.assertEqual("OK", x, msg)

    def assertCmdOk(self, cmd, *args, **kwargs):
        self.assertOk(self.cmd(cmd, *args, **kwargs))

    def assertExists(self, r, key, msg=None):
        self.assertTrue(r.exists(key), msg)

    def assertNotExists(self, r, key, msg=None):
        self.assertFalse(r.exists(key), msg)

    def retry_with_reload(self):
        return self.client.retry_with_rdb_reload()

    @contextlib.contextmanager
    def assertResponseError(self, msg=None):
        """
        Assert that a context block with a redis command triggers a redis error response.

        For Example:

            with self.assertResponseError():
                r.execute_command('non_existing_command')
        """

        try:
            yield
        except ResponseError:
            pass
        else:
            self.fail("Expected redis ResponseError " + (msg or ''))