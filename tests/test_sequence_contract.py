import unittest

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

import wire


class SequenceContractTests(unittest.TestCase):
    def test_seq_next_allows_zero_and_wraps(self):
        self.assertEqual(wire.seq_next(0), 1)
        self.assertEqual(wire.seq_next(2**32 - 1), 0)
        self.assertEqual(wire.seq_next(1), 2)


if __name__ == "__main__":
    unittest.main()
