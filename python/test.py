import random
import unittest
from similarity import Similarity

class TestSimilarityFunkcions(unittest.TestCase):
    def setUp(self):
        self.sim = Similarity(1,1)

    def test_getIndex(self):
        self.sim.indexes = {}
        self.assertEqual(0, self.sim.getIndex('aa'))
        self.sim.indexes = {'aa':0}
        self.assertEqual(0, self.sim.getIndex('aa'))
        self.sim.indexes = {'aa':0}
        self.assertEqual(1, self.sim.getIndex('bb'))
        self.sim.indexes = {'bb':0,'aa':1}
        self.assertEqual(0, self.sim.getIndex('bb'))
        self.sim.indexes = {'bb':0,'aa':1}
        self.assertEqual(1, self.sim.getIndex('aa'))
        self.sim.indexes = {1:0,'aa':1}
        self.assertEqual(0, self.sim.getIndex(1))
        self.sim.indexes = {'bb':0,'aa':1}
        self.assertEqual(1, self.sim.getIndex('aa'))

        
if __name__ == '__main__':
    unittest.main()
