from unittest import TestCase


class TestNetwork(TestCase):
    def test_check_url(self):
        from dpostools import api
        from dpostools import exceptions
        correct_url = 'http://146.185.144.47:4001'
        incorrect_url = '146.185.144.47'

        self.assertTrue(api.check_url(correct_url))
        self.assertRaises(exceptions.PeerFormatError, api.check_url, incorrect_url)

    def test_network_init_ark(self):
        from dpostools import api
        from dpostools import constants
        ark_network_peers = api.Network(network='ark').PEERS
        self.assertTrue(len(ark_network_peers) == len(constants.BlockHubPEERSArk))

        for x, y in zip(constants.BlockHubPEERSArk, ark_network_peers):
            self.assertEqual(y, 'http://{}:4001'.format(x))

    def test_add_peer(self):
        from dpostools import api
        from dpostools import exceptions
        from dpostools import constants

        correct_url = 'http://146.185.144.47:4001'
        incorrect_url = '146.185.144.47'
        ark_network = api.Network(network='ark')

        ark_network.add_peer(correct_url)

        self.assertTrue(len(ark_network.PEERS) == len(constants.BlockHubPEERSArk) + 1)

        self.assertRaises(exceptions.PeerFormatError, ark_network.add_peer, incorrect_url)

    def test_remove_peer(self):
        from dpostools import api
        from dpostools import constants
        correct_url = 'http://94.16.121.39:4001'
        ark_network = api.Network(network='ark')

        ark_network.remove_peer(correct_url)

        self.assertTrue(len(ark_network.PEERS) == len(constants.BlockHubPEERSArk) - 1)

    def test_clear_peer(self):
        from dpostools import api

        ark_network = api.Network(network='ark')

        self.assertFalse(ark_network.clear_peers())

    def test_status(self):
        from dpostools import api

        ark_network = api.Network(network='ark')
        status = ark_network.status()

        self.assertIsInstance(status['network_height'], int)

        for i in status['peer_status']:
            self.assertIsInstance(status['peer_status'][i]['height'], int)
            self.assertIsInstance(status['peer_status'][i]['status'], str)
            self.assertIsInstance(status['peer_status'][i]['version'], str)
            self.assertIsInstance(status['peer_status'][i]['delay'], int)

    def test_testnet_broadcast(self):
        """This is a devnet wallet. Please don't clear it, the dark isn't worth anything."""
        from dpostools import api

        dark_network = api.Network(network='dark')
        recipient_address = 'DA1SPukujJqfVqiGfq9yFUnnEucpxevgbA'
        secret = 'talk coral spatial wall pipe wolf orient attack soft favorite ordinary buzz'


        second_1_secret = 'master direct beyond defy render rhythm lonely apple wise enter way glow'
        second_2_secret = 'effort broccoli interest insect pact math snake culture marriage top envelope record'

        single_secret_tx = dark_network.broadcast_tx(
                                address=recipient_address,
                                amount=1,
                                secret=secret)

        self.assertTrue(single_secret_tx['success'])


        second_secret_tx = dark_network.broadcast_tx(
            address=recipient_address,
            amount=1,
            secret=second_1_secret,
            secondsecret=second_2_secret)

        self.assertTrue(second_secret_tx['success'])

    def test_mainnet_broadcast(self):
        """Not going to send actual ark here (ark is money, friend!)"""
        from dpostools import api
        ark_network = api.Network(network='ark')

        recipient = 'Aa5ASKhEpCv11vmQCysLvU3BXdEkkwpZZi'

        # this account holds 3e-8 ark
        secret = 'mule swamp solid false mango nothing example cover ozone cake mule patient'

        res = ark_network.broadcast_tx(address=recipient,
                                       amount=1,
                                       secret=secret)

        self.assertTrue(res['success'])
        self.assertFalse(res['responses']['success'])
        self.assertEqual(res['responses']['message'], 'Invalid transaction detected')
        self.assertEqual(res['responses']['error'], 'Account does not have enough ARK: Aa5ASKhEpCv11vmQCysLvU3BXdEkkwpZZi balance: 3e-8')