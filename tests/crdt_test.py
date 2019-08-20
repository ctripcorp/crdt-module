import sys
import os
dir_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(dir_path)

from core import random_str, LOCAL_GID, current_milli_time, BaseModuleTestCase


class CrdtTest(BaseModuleTestCase):

    def testCrdt(self):
        self.assertCmdOk('set', 'k', 'v')