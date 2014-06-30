# Copyright (c) 2013, University of Liverpool
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
#
# Author       : Jerome Fuselier
#

import unittest
from common import *
from irods import *

class testGetRodsEnv(iRODSTestCase):

    def test_rodsEnv(self):
        v1 = create_rodsEnv()
        v2 = create_rodsEnv()
        self.assertTrue(test_rodsEnv(v1, v2))


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(testGetRodsEnv))
    
    return suite


if __name__ == "__main__":
    unittest.TextTestRunner(verbosity=2).run(suite())