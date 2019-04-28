import os
import redis
import pytest
import time
import __builtin__
import math
import unittest
from rmtest import ModuleTestCase


class CrdtTests(ModuleTestCase('../crdt.so', module_args = ('CRDT.GID', '2'))):

    def testCmd(self):
        self.assertOk(self.cmd('set', 'key', 'val'))
        self.assertEquals('val', self.cmd('get', 'key'), "expected: val, but actual: {}".format(self.cmd('get', 'key')))


if __name__ == '__main__':
    unittest.main()

