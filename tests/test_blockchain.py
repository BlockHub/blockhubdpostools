from unittest import TestCase


class TestBlockchain(TestCase):
    def test_height(self):
        import dbtools

        height = dbtools.Blockchain.height()
        self.assertIsInstance(height, int)