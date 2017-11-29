from unittest import TestCase


class TestNodeArk(TestCase):
    def test_node_setup(self):
        import dbtools

        myarknode = dbtools.ArkNode(
            host='localhost',
            user='guestark',
            password='arkarkbarkbark',
            database='ark_mainnet',
        )

        self.assertIsInstance(myarknode, dbtools.ArkNode)
        self.assertIsInstance(myarknode.scheme, dict)

    def test_account_details(self):
        import dbtools

        myarknode = dbtools.ArkNode(
            host='localhost',
            user='guestark',
            password='arkarkbarkbark',
            database='ark_mainnet',
        )

        details = myarknode.account_details('AXx4bD2qrL1bdJuSjePawgJxQn825aNZZC')
        self.assertIsInstance(details, dict)
        self.assertIsNotNone(details)

    def test_node_height_details(self):
        import dbtools

        myarknode = dbtools.ArkNode(
            host='localhost',
            user='guestark',
            password='arkarkbarkbark',
            database='ark_mainnet',
        )

        height = myarknode.node_height_details()
        self.assertIsInstance(height, dict)
        self.assertIsNotNone(height)

    def all_delegates(self):
        import dbtools

        myarknode = dbtools.ArkNode(
            host='localhost',
            user='guestark',
            password='arkarkbarkbark',
            database='ark_mainnet',
        )

        all_delegates = myarknode.all_delegates()
        self.assertIsInstance(all_delegates, list)
        self.assertTrue(len(all_delegates) > 51)

    def test_current_delegates(self):
        import dbtools

        myarknode = dbtools.ArkNode(
            host='localhost',
            user='guestark',
            password='arkarkbarkbark',
            database='ark_mainnet',
        )

        delegates = myarknode.current_delegates()
        self.assertIsInstance(delegates, list)
        self.assertIsNotNone(delegates)
        # this test only works for Ark
        self.assertTrue(len(delegates) == 51)

    def test_payouts_to_address(self):
        import dbtools

        myarknode = dbtools.ArkNode(
            host='localhost',
            user='guestark',
            password='arkarkbarkbark',
            database='ark_mainnet',
        )

        payouts = myarknode.payouts_to_address('AJwHyHAArNmzGfmDnsJenF857ATQevg8HY')
        self.assertIsInstance(payouts, list)

    def test_transactions_from_address(self):
        import dbtools

        myarknode = dbtools.ArkNode(
            host='localhost',
            user='guestark',
            password='arkarkbarkbark',
            database='ark_mainnet',
        )

        transactions = myarknode.transactions_from_address('AJwHyHAArNmzGfmDnsJenF857ATQevg8HY')
        self.assertIsInstance(transactions, list)
        self.assertIsNotNone(transactions)

    def test_all_votes_by_address(self):
        import dbtools

        myarknode = dbtools.ArkNode(
            host='localhost',
            user='guestark',
            password='arkarkbarkbark',
            database='ark_mainnet',
        )
        all_votes = myarknode.all_votes_by_address('AJwHyHAArNmzGfmDnsJenF857ATQevg8HY')
        self.assertIsInstance(all_votes, list)
        self.assertIsNotNone(all_votes)

    def test_calculate_balance_over_time(self):
        import dbtools

        myarknode = dbtools.ArkNode(
            host='localhost',
            user='guestark',
            password='arkarkbarkbark',
            database='ark_mainnet',
        )
        # the normal address is a testing cold address I made specifically for unit testing
        normal_address = 'AXx4bD2qrL1bdJuSjePawgJxQn825aNZZC'

        balance_over_time_normal_address = myarknode.calculate_balance_over_time(normal_address)
        self.assertIsInstance(balance_over_time_normal_address, dict)
        self.assertIsNotNone(balance_over_time_normal_address)
        self.assertTrue(balance_over_time_normal_address[15813393] == 100000003)

        # the delegate address is shaman's, who stole his voters money. I do not expect him to ever start forging again
        # and thus consider that specific walllet as frozen
        delegate_address = 'AJRZHsHjqED3E3h55Ai9H6DtuoWUiBjLo7'

        balance_over_time_delegate_address = myarknode.calculate_balance_over_time(delegate_address)
        self.assertIsInstance(balance_over_time_delegate_address, dict)
        self.assertIsNotNone(balance_over_time_delegate_address)

        # shaman apparently left 1 ark satoshi on his account
        self.assertTrue(balance_over_time_delegate_address[19815336] == 1)