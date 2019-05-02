import os
import subprocess
import socket
import redis
import time
import sys
import random

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(PROJECT_ROOT)
MODULE_DIR = os.path.dirname(BASE_DIR)


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
                     '--loadmodule', MODULE_DIR + '/crdt.so']
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
