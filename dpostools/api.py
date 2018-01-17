"""Some additional functions related to arky by Moustikitos (pre-AIP11)"""

from dpostools import constants, exceptions
import arky.rest
import requests
import re
import random


def check_url(url):
    URL_REGEX = re.compile(
        r'^(?:http|ftp)s?://'  # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'  # domain...
        r'localhost|'  # localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
        r'(?::\d+)?'  # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)

    if not URL_REGEX.match(url):
        raise exceptions.PeerFormatError

    return True


class Network:
    """Node network specifications"""
    def __init__(self, network='ark'):
        self.network = network

        if network == 'ark':
            self.PEERS = ['http://{}:4001'.format(x) for x in constants.BlockHubPEERSArk]
        elif network == 'dark':
            self.PEERS = ['http://{}:4002'.format(x) for x in constants.BlockHubPEERSDark]

        arky.rest.use(network=self.network)

    def add_peer(self, peer):
        """
        Add a peer or multiple peers to the PEERS variable, takes a single string or a list.

        Format: 'http://{}:port'
        :param peer(list or string)
        """
        if type(peer) == list:
            for i in peer:
                check_url(i)
            self.PEERS.extend(peer)
        elif type(peer) == str:
            check_url(peer)
            self.PEERS.append(peer)

    def remove_peer(self, peer):
        """
        remove one or multiple peers from PEERS variable

        :param peer(list or string):
        """
        if type(peer) == list:
            for x in peer:
                check_url(x)
                for i in self.PEERS:
                    if x in i:
                        self.PEERS.remove(i)
        elif type(peer) == str:
            check_url(peer)
            for i in self.PEERS:
                if peer == i:
                    self.PEERS.remove(i)
        else:
            raise ValueError('peer paramater did not pass url validation')

    def clear_peers(self):
        """
        remove all PEERS from the PEERS variable
        """
        self.PEERS = []

    def status(self):
        """
        check the status of the network and the peers

        :return: network_height, peer_status
        """
        peer = random.choice(self.PEERS)
        peerdata = requests.get(url=peer + '/api/peers/').json()['peers']
        peers_status = {}

        networkheight = max([x['height'] for x in peerdata])

        for i in peerdata:
            if 'http://{}:4001'.format(i['ip']) in self.PEERS:
                peers_status.update({i['ip']: {
                    'height': i['height'],
                    'status': i['status'],
                    'version': i['version'],
                    'delay': i['delay'],
                }})

        return {
            'network_height': networkheight,
            'peer_status': peers_status
        }

    def broadcast_tx(self, address, amount, secret, secondsecret=None, vendorfield=''):
        """broadcasts a transaction to the peerslist"""
        arky.rest.cfg.peers = self.PEERS

        res = arky.core.sendTransaction(
            amount=amount,
            recipientId=address,
            secret=secret,
            secondSecret=secondsecret
        )

        success = False
        percentage = 0

        if res['broadcast']:
            success = True
            percentage = res['broadcast']

        return {
            'success': success,
            'responses': res,
            'percentage': percentage,
        }
