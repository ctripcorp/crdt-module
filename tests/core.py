import contextlib
import os
import random
import socket
import string
import subprocess
import sys
import time
import unittest

import redis
from redis import ResponseError

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


PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(PROJECT_ROOT)


def get_random_port():

    while True:
        port = random.randrange(1025, 10000)
        sock = socket.socket()
        try:
            sock.listen(port)
        except Exception, e:
            continue
        sock.close()
        return port


class RedisServer(object):

    def __init__(self, port=None, path=PROJECT_ROOT + '/redis-server', module_args=None):
        """
        :param port: port number to start the redis server on.
            Specify none to automatically generate
        :type port: int|None
        :param module_args: like ['CRDT.GID' '2']
        """
        if port is None:
            self.port = get_random_port()
        else:
            self.port = port

        self._client = None
        self.errored = False
        self.extra_args = [] if module_args is None else module_args
        self.path = path
        self.args = [self.path,
                     '--port', str(self.port),
                     '--loadmodule', BASE_DIR + '/crdt.so']
        self.args += self.extra_args
        self.process = None

    def _start_process(self):
        if self.process is not None:
            return

        args = self.args
        stdout = subprocess.PIPE
        stderr = subprocess.PIPE
        self.process = subprocess.Popen(
            args,
            stdin=sys.stdin,
            stdout=stdout,
            stderr=stderr
        )

        begin = time.time()
        while True:
            try:
                self.client().ping()
                break
            except (redis.ConnectionError, redis.ResponseError):
                self.process.poll()
                if self.process.returncode is not None:
                    raise RuntimeError(
                        "Process has exited with code {}\n. Redis output: {}"
                            .format(self.process.returncode, self._get_output()))

                if time.time() - begin > 300:
                    raise RuntimeError(
                        'Cannot initialize client (waited 5mins)')

                time.sleep(0.1)

    def _get_output(self):
        if not self.process:
            return ''
        return self.process.stdout.read()

    def start(self):
        """
        Start the server. To stop the server you should call stop()
        accordingly
        """
        self._start_process()

    def stop(self):
        if self.process is not None:
            self.process.terminate()
            while True:
                try:
                    self.client().ping()
                except (redis.ConnectionError, redis.ResponseError):
                    break
            time.sleep(0.1)

    def get_port(self):
        return self.port

    def client(self):
        if self._client is None:
            self._client = redis.Redis(host='127.0.0.1', port=self.port)
        return self._client
