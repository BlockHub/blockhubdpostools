from unittest import TestCase


host = 'localhost'
dbuser = 'postgres'
password = None
database = 'ark_mainnet'


class TestNodeArk(TestCase):
    def test_node_setup(self):
        from dpostools import dbtools

        myarknode = dbtools.ArkNode(
            host=host,
            user=dbuser,
            password=password,
            database=database,
        )

        self.assertIsInstance(myarknode, dbtools.ArkNode)
        self.assertIsInstance(myarknode.scheme, dict)

    def test_account_details(self):
        from dpostools import dbtools

        myarknode = dbtools.ArkNode(
            host=host,
            user=dbuser,
            password=password,
            database=database,
        )
        address = 'AXx4bD2qrL1bdJuSjePawgJxQn825aNZZC'
        details = myarknode.account_details(address)
        self.assertIsInstance(details, dict)
        self.assertIsNotNone(details)
        self.assertEqual(details['address'], address)

    def test_node_height_details(self):
        from dpostools import dbtools

        myarknode = dbtools.ArkNode(
            host=host,
            user=dbuser,
            password=password,
            database=database,
        )

        height = myarknode.node_height_details()
        self.assertIsInstance(height, dict)
        self.assertIsNotNone(height)

    def all_delegates(self):
        from dpostools import dbtools

        myarknode = dbtools.ArkNode(
            host=host,
            user=dbuser,
            password=password,
            database=database,
        )

        all_delegates = myarknode.all_delegates()
        self.assertIsInstance(all_delegates, dict)
        self.assertTrue(len(all_delegates) > 51)

    def test_current_delegates(self):
        from dpostools import dbtools

        myarknode = dbtools.ArkNode(
            host=host,
            user=dbuser,
            password=password,
            database=database,
        )

        delegates = myarknode.current_delegates()
        self.assertIsInstance(delegates, dict)
        self.assertIsNotNone(delegates)
        # this test only works for Ark
        self.assertTrue(len(delegates) == 51)

    def test_payouts_to_address(self):
        from dpostools import dbtools

        myarknode = dbtools.ArkNode(
            host=host,
            user=dbuser,
            password=password,
            database=database,
        )

        payouts = myarknode.payouts_to_address('AMbR3sWGzF3rVqBrgYRnAvxL2TVh44ZEft')
        self.assertIsInstance(payouts, dict)
        for i in payouts:
            self.assertTrue(i == payouts[i]['tx_id'])

    def test_transactions_from_address(self):
        from dpostools import dbtools

        myarknode = dbtools.ArkNode(
            host=host,
            user=dbuser,
            password=password,
            database=database,
        )

        transactions = myarknode.transactions_from_address('AJwHyHAArNmzGfmDnsJenF857ATQevg8HY')
        self.assertIsInstance(transactions, dict)
        self.assertIsNotNone(transactions)

    def test_all_votes_by_address(self):
        from dpostools import dbtools

        myarknode = dbtools.ArkNode(
            host=host,
            user=dbuser,
            password=password,
            database=database,
        )
        all_votes = myarknode.all_votes_by_address('AJwHyHAArNmzGfmDnsJenF857ATQevg8HY')
        self.assertIsInstance(all_votes, dict)
        self.assertIsNotNone(all_votes)

    def test_calculate_balance_over_time(self):
        from dpostools import dbtools

        myarknode = dbtools.ArkNode(
            host=host,
            user=dbuser,
            password=password,
            database=database,
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

    def test_get_last_out_transactions(self):
        from dpostools import dbtools

        myarknode = dbtools.ArkNode(
            host=host,
            user=dbuser,
            password=password,
            database=database,
        )

        last_payouts = myarknode.get_last_out_transactions('AJRZHsHjqED3E3h55Ai9H6DtuoWUiBjLo7')


        self.assertIsInstance(last_payouts, dict)
        self.assertIsNotNone(last_payouts)

        self.assertTrue(last_payouts[max(last_payouts.keys())]['recipient_id'] == 'Aa99TgGBMor5jssbzhUKHtDQMRfQukUeTM')

    def test_get_historic_voters(self):
        from dpostools import dbtools

        myarknode = dbtools.ArkNode(
            host=host,
            user=dbuser,
            password=password,
            database=database,
        )

        delegate_address = 'AJRZHsHjqED3E3h55Ai9H6DtuoWUiBjLo7'

        historic_voters = myarknode.get_historic_voters(address=delegate_address)
        self.assertIsInstance(historic_voters, dict)
        self.assertEqual(len(historic_voters), 38)

        for i in historic_voters:
            self.assertIsInstance(historic_voters[i]['address'], str)
            self.assertIsInstance(historic_voters[i]['timestamp'], int)
            self.assertIsInstance(historic_voters[i]['id'], str)
            self.assertIsInstance(historic_voters[i]['height'], int)

    def test_current_voters(self):
        from dpostools import dbtools

        myarknode = dbtools.ArkNode(
            host=host,
            user=dbuser,
            password=password,
            database=database,
        )

        delegate_address = 'AJRZHsHjqED3E3h55Ai9H6DtuoWUiBjLo7'

        current_voters = myarknode.get_current_voters(address=delegate_address)

        self.assertIsInstance(current_voters, dict)
        self.assertEqual(len(current_voters), 38)

        for i in current_voters:
            self.assertIsInstance(current_voters[i]['address'], str)
            self.assertIsInstance(current_voters[i]['timestamp'], int)
            self.assertIsInstance(current_voters[i]['id'], str)
            self.assertIsInstance(current_voters[i]['height'], int)

    def test_get_blocks(self):
        from dpostools import dbtools

        myarknode = dbtools.ArkNode(
            host=host,
            user=dbuser,
            password=password,
            database=database,
        )

        delegate_address = 'AJRZHsHjqED3E3h55Ai9H6DtuoWUiBjLo7'

        blocks = myarknode.get_blocks(delegate_address=delegate_address)

        self.assertIsInstance(blocks, dict)
        self.assertEqual(len(blocks), 4305)

        for i in blocks:
            self.assertIsInstance(blocks[i]['height'], int)
            self.assertIsInstance(blocks[i]['timestamp'], int)
            self.assertIsInstance(blocks[i]['total_fee'], int)
            self.assertIsInstance(blocks[i]['reward'], int)
            self.assertIsInstance(blocks[i]['id'], str)

    def test_transactions_vote_cluster(self):
        from dpostools import dbtools

        myarknode = dbtools.ArkNode(
            host=host,
            user=dbuser,
            password=password,
            database=database,
        )

        delegate_address = 'AJRZHsHjqED3E3h55Ai9H6DtuoWUiBjLo7'

        transactions = myarknode.get_transaction_vote_cluster(delegate_address=delegate_address)
        labelset = ['id', 'amount', 'timestamp', 'recipient_id', 'sender_id', 'rawasset', 'type', 'fee', 'block_id', 'height']

        self.assertIsInstance(transactions, dict)
        self.assertIsNotNone(transactions)

        for i in transactions:
            self.assertIsInstance(transactions[i]['amount'], int)
            self.assertIsInstance(transactions[i]['timestamp'], int)
            self.assertIsInstance(transactions[i]['type'], int)
            self.assertIsInstance(transactions[i]['fee'], int)
            self.assertIsInstance(transactions[i]['height'], int)
            self.assertIsInstance(transactions[i]['id'], str)
            self.assertIsInstance(transactions[i]['recipient_id'], str)
            self.assertIsInstance(transactions[i]['sender_id'], str)
            self.assertIsInstance(transactions[i]['rawasset'], str)
            self.assertIsInstance(transactions[i]['block_id'], str)

    def test_tbw(self):
        from dpostools import dbtools

        delegate_address = 'AJRZHsHjqED3E3h55Ai9H6DtuoWUiBjLo7'


        myarknode = dbtools.ArkNode(
            host=host,
            user=dbuser,
            password=password,
            database=database,
        )

        payouts = myarknode.tbw(delegate_address=delegate_address)[0]

        for i in payouts:
            print(i, payouts[i])

        self.assertIsInstance(payouts, dict)


