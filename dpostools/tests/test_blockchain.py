from unittest import TestCase


class TestBlockchain(TestCase):
    def test_height(self):
        from dpostools import dbtools

        height = dbtools.Blockchain.height()
        self.assertIsInstance(height, int)