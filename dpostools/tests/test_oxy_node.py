from unittest import TestCase


class TestNodeOxycoin(TestCase):
    def test_node_setup(self):
        from dpostools import dbtools

        mynode = dbtools.OxycoinNode(
            host='localhost',
            user='guestark',
            password='arkarkbarkbark',
        )

        self.assertIsInstance(mynode, dbtools.OxycoinNode)
        self.assertIsInstance(mynode.scheme, dict)

    def test_account_details(self):
        from dpostools import dbtools

        mynode = dbtools.OxycoinNode(
            host='localhost',
            user='guestark',
            password='arkarkbarkbark',
        )

        details = mynode.account_details('2660824342794420041X')
        self.assertIsInstance(details, dict)
        self.assertIsNotNone(details)

    def test_node_height_details(self):
        from dpostools import dbtools

        mynode = dbtools.OxycoinNode(
            host='localhost',
            user='guestark',
            password='arkarkbarkbark',
        )

        node_height_details = mynode.node_height_details()
        self.assertIsInstance(node_height_details, dict)
        self.assertIsNotNone(node_height_details)

    def all_delegates(self):
        from dpostools import dbtools

        mynode = dbtools.OxycoinNode(
            host='localhost',
            user='guestark',
            password='arkarkbarkbark',
        )

        all_delegates = mynode.all_delegates()
        self.assertIsInstance(all_delegates, list)
        self.assertTrue(len(all_delegates) > 201)

    def test_current_delegates(self):
        from dpostools import dbtools

        mynode = dbtools.OxycoinNode(
            host='localhost',
            user='guestark',
            password='arkarkbarkbark',
        )

        delegates = mynode.current_delegates()
        self.assertIsInstance(delegates, list)
        self.assertIsNotNone(delegates)
        # this test only works for Ark
        self.assertTrue(len(delegates) == 201)

    def test_payouts_to_address(self):
        from dpostools import dbtools

        mynode = dbtools.OxycoinNode(
            host='localhost',
            user='guestark',
            password='arkarkbarkbark',
        )

        payouts = mynode.payouts_to_address('2660824342794420041X')
        self.assertIsInstance(payouts, list)

    def test_transactions_from_address(self):
        from dpostools import dbtools

        mynode = dbtools.OxycoinNode(
            host='localhost',
            user='guestark',
            password='arkarkbarkbark',
        )

        transactions = mynode.transactions_from_address('2660824342794420041X')
        self.assertIsInstance(transactions, list)
        self.assertIsNotNone(transactions)


    def test_all_votes_by_address(self):
        from dpostools import dbtools

        mynode = dbtools.OxycoinNode(
            host='localhost',
            user='guestark',
            password='arkarkbarkbark',
        )
        all_votes = mynode.all_votes_by_address('9656507962620078912X')
        self.assertIsInstance(all_votes, list)
        self.assertIsNotNone(all_votes)