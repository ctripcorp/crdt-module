import os
import sys

dir_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(dir_path)

from core import random_str, LOCAL_GID, current_milli_time, BaseModuleTestCase


class CrdtRegisterTest(BaseModuleTestCase):

    def testBasicSet(self):
        self.assertCmdOk('set', 'key', 'val')
        self.assertCmdOk('set', 'key', 'val')

    def testBasicGet(self):
        self.assertCmdOk('set', 'key', 'val')
        self.assertEqual('val', self.cmd('get', 'key'))

    def testMultiSetPotentialMemLeak(self):
        for i in range(0, 1000):
            self.assertCmdOk('set', 'key', random_str(100))

    def testMultiSetGetPotentinalMemLeak(self):
        for i in range(0, 100):
            val = random_str(100)
            self.assertCmdOk('set', 'key', val)
            self.assertEqual(val, self.cmd('get', 'key'))

    def testCrdtGet(self):
        self.cmd('set', 'key', 'val')
        _val, _gid, _timestmap = self.cmd('crdt.get', 'key')
        self.assertEqual(LOCAL_GID, _gid)
        # diff less than 5 ms
        self.assertTrue(current_milli_time() - _timestmap < 5)

    def testCrdtSet(self):
        self.assertCmdOk('crdt.set', 'key', LOCAL_GID, current_milli_time(), 'val')
