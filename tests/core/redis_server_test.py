import time
import unittest
import redis

from tests.core.redis_server import RedisServer


class TestRedisServer(unittest.TestCase):

    def testRedisServer(self):
        redis_server = RedisServer(port=6379, module_args=('CRDT.GID', '2'))
        redis_server.start()
        r = redis.Redis(host='127.0.0.1', port=redis_server.get_port())
        r.set('key', 'val')
        print r.execute_command('crdt.get', 'key')
        print r.info('server')
        redis_server.stop()
        while True:
            try:
                r.ping()
            except (redis.ConnectionError, redis.ResponseError):
                break
            time.sleep(0.1)
