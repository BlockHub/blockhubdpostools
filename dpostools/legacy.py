"""Legacy arkdbtools code, because I haven't gotten around to rewriting everything yet."""
import psycopg2
from arkdbtools import utils
import arkdbtools.config as c
from collections import namedtuple
import binascii
import datetime
import logging
import requests.exceptions as rq


if c.LOGGING['USE']:
    logger = logging.getLogger(__name__)
    handler = c.LOGGING['HANDLER']
    formatter = logging.Formatter(c.LOGGING['FORMAT'])
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(c.LOGGING['LEVEL'])
else:
    logger = logging.getLogger(__name__)
    logger.propagate = False


class InputError(Exception):
    pass


class AllocationError(Exception):
    pass


class TxParameterError(Exception):
    pass


class ApiError(Exception):
    pass


class NodeDbError(Exception):
    pass


class NegativeBalanceError(Exception):
    pass


class ParseError(Exception):
    pass


def set_connection(host=None, database=None, user=None, password=None):
    """Set connection parameters. Call set_connection with no arguments to clear."""
    c.CONNECTION['HOST'] = host
    c.CONNECTION['DATABASE'] = database
    c.CONNECTION['USER'] = user
    c.CONNECTION['PASSWORD'] = password


def set_delegate(address=None, pubkey=None, secret=None):
    """Set delegate parameters. Call set_delegate with no arguments to clear."""
    c.DELEGATE['ADDRESS'] = address
    c.DELEGATE['PUBKEY'] = pubkey
    c.DELEGATE['PASSPHRASE'] = secret


def set_calculation(blacklist=None, exceptions=None, max_amount=float('inf'), share_fees=False ):
    if not exceptions:
        exceptions = {'address': {'replace': 'int else None'}}

    c.CALCULATION_SETTINGS['BLACKLIST'] = blacklist
    c.CALCULATION_SETTINGS['EXCEPTIONS'] = exceptions
    c.CALCULATION_SETTINGS['MAX'] = max_amount
    c.CALCULATION_SETTINGS['SHARE_FEES'] = share_fees


def set_sender(default_share=0, cover_fees=False, share_percentage_exceptions=None, timestamp_brackets=None,
               min_payout_daily=0, min_payout_weekly=0, min_payout_monthly=0, day_weekly_payout=5, day_monthly_payout=10,
               payoutsender_test=True, sender_exception=None, wait_time_day=0, wait_time_week=0, wait_time_month=0,
               smartbridge=''):


    c.SENDER_SETTINGS['DEFAULT_SHARE'] = default_share
    c.SENDER_SETTINGS['COVER_FEES'] = cover_fees
    c.SENDER_SETTINGS['SHARE_PERCENTAGE_EXCEPTIONS'] = share_percentage_exceptions
    c.SENDER_SETTINGS['TIMESTAMP_BRACKETS'] = timestamp_brackets
    c.SENDER_SETTINGS['MIN_PAYOUT_DAILY'] = min_payout_daily
    c.SENDER_SETTINGS['MIN_PAYOUT_WEEKLY'] = min_payout_weekly
    c.SENDER_SETTINGS['MIN_PAYOUT_MONTHLY'] = min_payout_monthly
    c.SENDER_SETTINGS['DAY_WEEKLY_PAYOUT'] = day_weekly_payout
    c.SENDER_SETTINGS['DAY_MONTHLY_PAYOUT'] = day_monthly_payout
    c.SENDER_SETTINGS['PAYOUTSENDER_TEST'] = payoutsender_test
    c.SENDER_SETTINGS['SENDER_EXCEPTIONS'] = sender_exception
    c.SENDER_SETTINGS['WAIT_TIME_DAY'] = wait_time_day
    c.SENDER_SETTINGS['WAIT_TIME_WEEK'] = wait_time_week
    c.SENDER_SETTINGS['WAIT_TIME_MONTH'] = wait_time_month
    c.SENDER_SETTINGS['SMARTBRIDGE'] = smartbridge


class DbConnection:
    def __init__(self):
        try:
            self._conn = psycopg2.connect(
                host=c.CONNECTION['HOST'],
                database=c.CONNECTION['DATABASE'],
                user=c.CONNECTION['USER'],
                password=c.CONNECTION['PASSWORD'])
        except Exception as e:
            logger.exception('failed to connect to ark-node: {}'.format(e))
            raise e

    def connection(self):
        return self._conn


class DbCursor:
    def __init__(self, dbconnection=None):
        if not dbconnection:
            dbconnection = DbConnection()
        self._cur = dbconnection.connection().cursor()

    def execute(self, qry, *args):
        self._cur.execute(qry, *args)

    def fetchall(self):
        return self._cur.fetchall()

    def fetchone(self):
        return self._cur.fetchone()

    def execute_and_fetchall(self, qry, *args):
        self.execute(qry, *args)
        return self._cur.fetchall()

    def execute_and_fetchone(self, qry, *args):
        self.execute(qry, *args)
        return self._cur.fetchone()

class Node:
    @staticmethod
    def height():
        res = None
        try:
            res = DbCursor().execute_and_fetchone("""
                            SELECT max(blocks."height") 
                            FROM blocks
            """)[0]
        except Exception as e:
            logger.exception(e)
            pass
        if not res:
            logger.fatal('failed to receive a response from the ark-node')
            raise NodeDbError('failed to receive a response from the ark-node: {}'.format(e))
        return res


    @staticmethod
    def max_timestamp():
        # Fetch the max timestamp as it occurs in table blocks, or return
        # a previously cached value.
        try:
            r = DbCursor().execute_and_fetchone("""
                    SELECT max(timestamp) 
                    FROM blocks
            """)[0]
        except Exception as e:
            logger.exception(e)
            raise NodeDbError('failed to receive a response from the ark-node: {}'.format(e))

        if not r:
            logger.fatal('failed to get max timestamp from node. {}'.format(DbCursor()))
            raise NodeDbError('failed to get max timestamp from node.')
        return r


class Address:

    @staticmethod
    def payout(address):
        """returns all received transactions between the address and registered delegate accounts
        ORDER by timestamp ASC."""
        qry = DbCursor().execute_and_fetchall("""
                SELECT DISTINCT transactions."id", transactions."amount",
                       transactions."timestamp", transactions."recipientId",
                       transactions."senderId", transactions."rawasset",
                       transactions."type", transactions."fee"
                FROM transactions, delegates
                WHERE transactions."senderId" IN (
                  SELECT transactions."senderId" 
                  FROM transactions, delegates 
                  WHERE transactions."id" = delegates."transactionId"
                )
                AND transactions."recipientId" = '{}'
                ORDER BY transactions."timestamp" ASC""".format(address))

        Transaction = namedtuple(
            'transaction',
            'id amount timestamp recipientId senderId rawasset type fee')
        named_transactions = []

        for i in qry:
            tx_id = Transaction(
                id=i[0],
                amount=i[1],
                timestamp=i[2],
                recipientId=i[3],
                senderId=i[4],
                rawasset=i[5],
                type=i[6],
                fee=i[7],
            )

            named_transactions.append(tx_id)
        return named_transactions

    @staticmethod
    def transactions(address):
        """Returns a list of named tuples of all transactions for an address.
        Scheme:
        'transaction',
            'id amount timestamp recipientId senderId rawasset type fee'"""
        qry = DbCursor().execute_and_fetchall("""
        SELECT transactions."id", transactions."amount",
               transactions."timestamp", transactions."recipientId",
               transactions."senderId", transactions."rawasset",
               transactions."type", transactions."fee"
        FROM transactions
        WHERE transactions."senderId" = '{0}'
        OR transactions."recipientId" = '{0}'
        ORDER BY transactions."timestamp" ASC""".format(address))

        Transaction = namedtuple(
            'transaction',
            'id amount timestamp recipientId senderId rawasset type fee')
        named_transactions = []

        for i in qry:
            tx_id = Transaction(
                id=i[0],
                amount=i[1],
                timestamp=i[2],
                recipientId=i[3],
                senderId=i[4],
                rawasset=i[5],
                type=i[6],
                fee=i[7],
                )

            named_transactions.append(tx_id)
        return named_transactions

    @staticmethod
    def votes(address):
        """Returns a list of namedtuples all votes made by an address, {(+/-)pubkeydelegate:timestamp}, timestamp DESC"""
        qry = DbCursor().execute_and_fetchall("""
           SELECT votes."votes", transactions."timestamp"
           FROM votes, transactions
           WHERE transactions."id" = votes."transactionId"
           AND transactions."senderId" = '{}'
           ORDER BY transactions."timestamp" DESC
        """.format(address))

        Vote = namedtuple(
            'vote',
            'direction delegate timestamp')
        res = []
        for i in qry:
            if i[0][0] == '+':
                direction = True
            elif i[0][0] == '-':
                direction = False
            else:
                logger.fatal('failed to interpret direction for: {}'.format(i))
                raise ParseError('failed to interpret direction of vote for: {}'.format(i))
            vote = Vote(
                direction=direction,
                delegate=i[0][1:],
                timestamp=i[1],
            )
            res.append(vote)
        return res

    @staticmethod
    def balance(address):
        """
        Takes a single address and returns the current balance.
        """
        txhistory = Address.transactions(address)
        balance = 0
        for i in txhistory:
            if i.recipientId == address:
                balance += i.amount
            if i.senderId == address:
                balance -= (i.amount + i.fee)

        delegates = Delegate.delegates()
        for i in delegates:
            if address == i.address:
                forged_blocks = Delegate.blocks(i.pubkey)
                for block in forged_blocks:
                    balance += (block.reward + block.totalFee)

        if balance < 0:
            height = Node.height()
            logger.fatal('Negative balance for address {0}, Nodeheight: {1)'.format(address, height))
            raise NegativeBalanceError('Negative balance for address {0}, Nodeheight: {1)'.format(address, height))
        return balance

    @staticmethod
    def balance_over_time(address):
        """returns a list of named tuples,  x.timestamp, x.amount including block rewards"""
        forged_blocks = None
        txhistory = Address.transactions(address)
        delegates = Delegate.delegates()
        for i in delegates:
            if address == i.address:
                forged_blocks = Delegate.blocks(i.pubkey)

        balance_over_time = []
        balance = 0
        block = 0

        Balance = namedtuple(
            'balance',
            'timestamp amount')

        for tx in txhistory:
            if forged_blocks:
                while forged_blocks[block].timestamp <= tx.timestamp:
                    balance += (forged_blocks[block].reward + forged_blocks[block].totalFee)
                    balance_over_time.append(Balance(timestamp=forged_blocks[block].timestamp, amount=balance))
                    block += 1

            if tx.senderId == address:
                balance -= (tx.amount + tx.fee)
                res = Balance(timestamp=tx.timestamp, amount=balance)
                balance_over_time.append(res)
            if tx.recipientId == address:
                balance += tx.amount
                res = Balance(timestamp=tx.timestamp, amount=balance)
                balance_over_time.append(res)

        if forged_blocks and block <= len(forged_blocks) - 1:
            if forged_blocks[block].timestamp > txhistory[-1].timestamp:
                for i in forged_blocks[block:]:
                    balance += (i.reward + i.totalFee)
                    res = Balance(timestamp=i.timestamp, amount=balance)
                    balance_over_time.append(res)

        return balance_over_time


class Delegate:

    @staticmethod
    def delegates():
        """returns a list of named tuples of all delegates.
        {username: {'pubkey':pubkey, 'timestamp':timestamp, 'address':address}}"""
        qry = DbCursor().execute_and_fetchall("""
            SELECT delegates."username", delegates."transactionId", transactions."timestamp", transactions."senderId", 
            transactions."senderPublicKey" 
            FROM transactions
            JOIN delegates ON transactions."id" = delegates."transactionId"
        """)

        Delegate = namedtuple(
            'delegate',
            'username pubkey timestamp address transactionId')
        res = []
        for i in qry:
            registration = Delegate(
                username=i[0],
                pubkey=binascii.hexlify(i[4]).decode("utf-8"),
                timestamp=i[2],
                address=i[3],
                transactionId=i[1]
            )
            res.append(registration)
        return res

    @staticmethod
    def lastpayout(delegate_address, blacklist=None):
        '''
        Assumes that all send transactions from a delegate are payouts.
        Use blacklist to remove rewardwallet and other transactions if the
        address is not a voter. blacklist can contain both addresses and transactionIds'''
        if blacklist and len(blacklist) > 1:
            command_blacklist = 'NOT IN ' + str(tuple(blacklist))
        elif blacklist and len(blacklist) == 1:
            command_blacklist = '!= ' + "'" + blacklist[0] + "'"
        else:
            command_blacklist = "!= 'nothing'"
        qry = DbCursor().execute_and_fetchall("""
                    SELECT ts."recipientId", ts."id", ts."timestamp"
                    FROM transactions ts,
                      (SELECT MAX(transactions."timestamp") AS max_timestamp, transactions."recipientId"
                       FROM transactions
                       WHERE transactions."senderId" = '{0}'
                       AND transactions."id" {1}
                       GROUP BY transactions."recipientId") maxresults
                    WHERE ts."recipientId" = maxresults."recipientId"
                    AND ts."recipientId" {1}
                    AND ts."timestamp"= maxresults.max_timestamp;

                    """.format(delegate_address, command_blacklist))
        result = []

        Payout = namedtuple(
            'payout',
            'address id timestamp')

        for i in qry:
            payout = Payout(
                address=i[0],
                id=i[1],
                timestamp=i[2]
            )
            result.append(payout)
        return result

    @staticmethod
    def votes(delegate_pubkey):
        """returns every address that has voted for a delegate.
        Current voters can be obtained using voters. ORDER BY timestamp ASC"""
        qry = DbCursor().execute_and_fetchall("""
                 SELECT transactions."recipientId", transactions."timestamp"
                 FROM transactions, votes
                 WHERE transactions."id" = votes."transactionId"
                 AND votes."votes" = '+{}'
                 ORDER BY transactions."timestamp" ASC;
        """.format(delegate_pubkey))

        Voter = namedtuple(
            'voter',
            'address timestamp')
        voters = []
        for i in qry:
            voter = Voter(
                address=i[0],
                timestamp=i[1]
                          )
            voters.append(voter)
        return voters

    @staticmethod
    def unvotes(delegate_pubkey):
        qry = DbCursor().execute_and_fetchall("""
                         SELECT transactions."recipientId", transactions."timestamp"
                         FROM transactions, votes
                         WHERE transactions."id" = votes."transactionId"
                         AND votes."votes" = '-{}'
                         ORDER BY transactions."timestamp" ASC;
                """.format(delegate_pubkey))

        Voter = namedtuple(
            'voter',
            'address timestamp')

        unvoters = []
        for i in qry:
            unvoter = Voter(
                address=i[0],
                timestamp=i[1]
            )
            unvoters.append(unvoter)
        return unvoters

    @staticmethod
    # todo make this into a single SQL call and move the logic to the DB to increase efficiency
    def voters(delegate_pubkey=None):
        if not delegate_pubkey:
            delegate_pubkey = c.DELEGATE['PUBKEY']
        votes = Delegate.votes(delegate_pubkey)
        unvotes = Delegate.unvotes(delegate_pubkey)
        for count, i in enumerate(votes):
            for x in unvotes:
                if i.address == x.address and i.timestamp < x.timestamp:
                    del votes[count]
        return votes

    @staticmethod
    def blocks(delegate_pubkey=None, max_timestamp=None):
        """returns a list of named tuples of all blocks forged by a delegate.
        if delegate_pubkey is not specified, set_delegate needs to be called in advance.
        max_timestamp can be configured to retrieve blocks up to a certain timestamp."""

        if not delegate_pubkey:
            delegate_pubkey = c.DELEGATE['PUBKEY']
        if max_timestamp:
            max_timestamp_sql = """ blocks."timestamp" <= {} AND""".format(max_timestamp)
        else:
            max_timestamp_sql = ''

        qry = DbCursor().execute_and_fetchall("""
             SELECT blocks."timestamp", blocks."height", blocks."id", blocks."totalFee", blocks."reward"
             FROM blocks
             WHERE {0} blocks."generatorPublicKey" = '\\x{1}'
             ORDER BY blocks."timestamp" 
             ASC""".format(
            max_timestamp_sql,
            delegate_pubkey))

        Block = namedtuple('block',
                           'timestamp height id totalFee reward')
        block_list = []
        for block in qry:
            block_value = Block(timestamp=block[0],
                                height=block[1],
                                id=block[2],
                                totalFee=block[3],
                                reward=block[4])
            block_list.append(block_value)

        return block_list

    @staticmethod
    def share(passphrase=None, last_payout=None, start_block=0, del_pubkey=None, del_address=None):
        """Calculate the true blockweight payout share for a given delegate,
        assuming no exceptions were made for a voter. last_payout is a map of addresses and timestamps:
        {address: timestamp}. If no argument are given, it will start the calculation at the first forged block
        by the delegate, generate a last_payout from transaction history, and use the set_delegate info.

        If a passphrase is provided, it is only used to generate the adddress and keys, no transactions are sent.
        (Still not recommended unless you know what you are doing, version control could store your passphrase for example;
        very risky)
        """
        logger.info('starting share calculation using settings: {0} {1}'.format(c.DELEGATE, c.CALCULATION_SETTINGS))


        delegate_pubkey = c.DELEGATE['PUBKEY']
        delegate_address = c.DELEGATE['ADDRESS']

        if del_pubkey and del_address:
            delegate_address = del_address
            delegate_pubkey = del_pubkey

        logger.info('Starting share calculation, using address:{0}, pubkey:{1}'.format(delegate_address, delegate_pubkey))

        max_timestamp = Node.max_timestamp()
        logger.info('Share calculation max_timestamp = {}'.format(max_timestamp))

        # utils function
        transactions = get_transactionlist(
                            delegate_pubkey=delegate_pubkey
        )

        votes = Delegate.votes(delegate_pubkey)

        # create a map of voters
        voter_dict = {}
        for voter in votes:
            voter_dict.update({voter.address: {
                'balance': 0.0,
                'status': False,
                'last_payout': voter.timestamp,
                'share': 0.0,
                'vote_timestamp': voter.timestamp,
                'blocks_forged': []}
            })

        # check if a voter is/used to be a forging delegate
        delegates = Delegate.delegates()
        for i in delegates:
            if i.address in voter_dict:
                logger.info('A registered delegate is a voter: {0}, {1}, {2}'.format(i.username, i.address, i.pubkey))
                try:
                    blocks_by_voter = Delegate.blocks(i.pubkey)
                    voter_dict[i.address]['blocks_forged'].extend(Delegate.blocks(i.pubkey))
                    logger.info('delegate {0} has forged {1} blocks'.format(i.username, len(blocks_by_voter)))
                except Exception:
                    logger.info('delegate {} has not forged any blocks'.format(i))
                    pass
        try:
            for i in c.CALCULATION_SETTINGS['BLACKLIST']:
                voter_dict.pop(i)
                logger.debug('popped {} from calculations'.format(i))
        except Exception:
            pass

        if not last_payout:
            last_payout = Delegate.lastpayout(delegate_address)
            for payout in last_payout:
                try:
                    voter_dict[payout.address]['last_payout'] = payout.timestamp
                except Exception:
                    pass
        elif type(last_payout) is int:
            for address in voter_dict:
                if address['vote_timestamp'] < last_payout:
                    address['last_payout'] = last_payout
        elif type(last_payout) is dict:
            for payout in last_payout:
                try:
                    voter_dict[payout.address]['last_payout'] = payout.timestamp
                except Exception:
                    pass
        else:
            logger.fatal('last_payout object not recognised: {}'.format(type(last_payout)))
            raise InputError('last_payout object not recognised: {}'.format(type(last_payout)))

        # get all forged blocks of delegate:
        blocks = Delegate.blocks(max_timestamp=max_timestamp,
                                 delegate_pubkey=delegate_pubkey)

        block_nr = start_block
        chunk_dict = {}
        reuse = False
        try:
            for tx in transactions:
                while tx.timestamp > blocks[block_nr].timestamp:
                    if reuse:
                        block_nr += 1
                        for x in chunk_dict:
                            voter_dict[x]['share'] += chunk_dict[x]
                        continue
                    block_nr += 1
                    poolbalance = 0
                    chunk_dict = {}
                    for i in voter_dict:
                        balance = voter_dict[i]['balance']

                        try:
                            if voter_dict[i]['balance'] > c.CALCULATION_SETTINGS['MAX']:
                                balance = c.CALCULATION_SETTINGS['MAX']
                        except Exception:
                            pass

                        try:
                            if balance > c.CALCULATION_SETTINGS['EXCEPTIONS'][i]['REPLACE']:
                                balance = c.CALCULATION_SETTINGS['EXCEPTIONS'][i]['REPLACE']
                        except Exception:
                            pass

                        try:
                            for x in voter_dict[i]['blocks_forged']:
                                if x.timestamp < blocks[block_nr].timestamp:
                                    voter_dict[i]['balance'] += (x.reward + x.totalFee)
                                    voter_dict[i]['blocks_forged'].remove(x)
                            balance = voter_dict[i]['balance']
                        except Exception:
                            pass

                        if voter_dict[i]['status']:
                            if not voter_dict[i]['balance'] < -20 * c.ARK:
                                poolbalance += balance
                            else:
                                logger.fatal('balance lower than zero for: {0}'.format(i))
                                raise NegativeBalanceError('balance lower than zero for: {0}'.format(i))

                    for i in voter_dict:
                        balance = voter_dict[i]['balance']

                        if voter_dict[i]['balance'] > c.CALCULATION_SETTINGS['MAX']:
                            balance = c.CALCULATION_SETTINGS['MAX']

                        try:
                            if balance > c.CALCULATION_SETTINGS['EXCEPTIONS'][i]['REPLACE']:
                                balance = c.CALCULATION_SETTINGS['EXCEPTIONS'][i]['REPLACE']
                        except Exception:
                            pass

                        if voter_dict[i]['status'] and voter_dict[i]['last_payout'] < blocks[block_nr].timestamp:
                            if c.CALCULATION_SETTINGS['SHARE_FEES']:
                                share = (balance/poolbalance) * (blocks[block_nr].reward +
                                                                 blocks[block_nr].totalFee)
                            else:
                                share = (balance/poolbalance) * blocks[block_nr].reward
                            voter_dict[i]['share'] += share
                            chunk_dict.update({i: share})
                    reuse = True


                # parsing a transaction
                minvote = '{{"votes":["-{0}"]}}'.format(delegate_pubkey)
                plusvote = '{{"votes":["+{0}"]}}'.format(delegate_pubkey)

                reuse = False

                if tx.recipientId in voter_dict:
                    voter_dict[tx.recipientId]['balance'] += tx.amount
                if tx.senderId in voter_dict:
                    voter_dict[tx.senderId]['balance'] -= (tx.amount + tx.fee)
                if tx.senderId in voter_dict and tx.type == 3 and plusvote in tx.rawasset:
                    voter_dict[tx.senderId]['status'] = True
                if tx.senderId in voter_dict and tx.type == 3 and minvote in tx.rawasset:
                    voter_dict[tx.senderId]['status'] = False

            remaining_blocks = len(blocks) - block_nr - 1
            for i in range(remaining_blocks):
                for x in chunk_dict:
                    voter_dict[x]['share'] += chunk_dict[x]

        # an IndexError occurs if max(transactions.timestamp) > max(blocks.timestamp) This means we parsed every block
        except IndexError:
            pass

        for i in voter_dict:
            logger.info("{0}  {1}  {2}  {3}  {4}".format(i,
                                                         voter_dict[i]['share'],
                                                         voter_dict[i]['status'],
                                                         voter_dict[i]['last_payout'],
                                                         voter_dict[i]['vote_timestamp']))
        return voter_dict, max_timestamp


    @staticmethod
    def trueshare(start_block=0, del_pubkey=None, del_address=None, blacklist=None, share_fees=False):
        '''
        Legacy TBW script (still pretty performant, but has some quirky behavior when forging delegates are amongst
        your voters)

        :param int start_block: block from which we start adding to the share (we calculate balances from block 0 anyways)
        :param str del_pubkey: delegate public key as is presented in the ark wallet
        :param str del_address: delegate address
        :param list blacklist: blacklist for addresses to be removed BEFORE calculation. Their share is removed from the pool balance
        :param bool share_fees: if tx fees should be shared as well.

        '''

        delegate_pubkey = c.DELEGATE['PUBKEY']
        delegate_address = c.DELEGATE['ADDRESS']

        if del_pubkey and del_address:
            delegate_address = del_address
            delegate_pubkey = del_pubkey

        max_timestamp = Node.max_timestamp()

        # utils function
        transactions = get_transactionlist(
            delegate_pubkey=delegate_pubkey
        )

        votes = Delegate.votes(delegate_pubkey)

        # create a map of voters
        voter_dict = {}
        for voter in votes:
            voter_dict.update({voter.address: {
                'balance': 0.0,
                'status': False,
                'last_payout': voter.timestamp,
                'share': 0.0,
                'vote_timestamp': voter.timestamp,
                'blocks_forged': []}
            })

        try:
            for i in blacklist:
                voter_dict.pop(i)
        except Exception:
            pass

        # check if a voter is/used to be a forging delegate
        delegates = Delegate.delegates()
        for i in delegates:
            if i.address in voter_dict:
                try:
                    blocks_by_voter = Delegate.blocks(i.pubkey)
                    voter_dict[i.address]['blocks_forged'].extend(Delegate.blocks(i.pubkey))
                except Exception:
                    pass

        last_payout = Delegate.lastpayout(delegate_address)
        for payout in last_payout:
            try:
                voter_dict[payout.address]['last_payout'] = payout.timestamp
            except Exception:
                pass

        blocks = Delegate.blocks(delegate_pubkey)
        block_nr = start_block
        chunk_dict = {}
        reuse = False
        try:
            for tx in transactions:
                while tx.timestamp > blocks[block_nr].timestamp:
                    if reuse:
                        block_nr += 1
                        for x in chunk_dict:
                            voter_dict[x]['share'] += chunk_dict[x]
                        continue

                    block_nr += 1
                    poolbalance = 0
                    chunk_dict = {}
                    for i in voter_dict:
                        balance = voter_dict[i]['balance']

                        #checks if a delegate that votes for us is has forged blocks in the mean time
                        try:
                            for x in voter_dict[i]['blocks_forged']:
                                if x.timestamp < blocks[block_nr].timestamp:
                                    voter_dict[i]['balance'] += (x.reward + x.totalFee)
                                    voter_dict[i]['blocks_forged'].remove(x)
                            balance = voter_dict[i]['balance']
                        except Exception:
                            pass

                        if voter_dict[i]['status']:
                            if not voter_dict[i]['balance'] < -20 * c.ARK:
                                poolbalance += balance
                            else:
                                raise NegativeBalanceError('balance lower than zero for: {0}'.format(i))

                    for i in voter_dict:
                        balance = voter_dict[i]['balance']

                        if voter_dict[i]['status'] and voter_dict[i]['last_payout'] < blocks[block_nr].timestamp:
                            if share_fees:
                                share = (balance / poolbalance) * (blocks[block_nr].reward +
                                                                   blocks[block_nr].totalFee)
                            else:
                                share = (balance / poolbalance) * blocks[block_nr].reward
                            voter_dict[i]['share'] += share
                            chunk_dict.update({i: share})
                    reuse = True

                # parsing a transaction
                minvote = '{{"votes":["-{0}"]}}'.format(delegate_pubkey)
                plusvote = '{{"votes":["+{0}"]}}'.format(delegate_pubkey)

                reuse = False

                if tx.recipientId in voter_dict:
                    voter_dict[tx.recipientId]['balance'] += tx.amount
                if tx.senderId in voter_dict:
                    voter_dict[tx.senderId]['balance'] -= (tx.amount + tx.fee)
                if tx.senderId in voter_dict and tx.type == 3 and plusvote in tx.rawasset:
                    voter_dict[tx.senderId]['status'] = True
                if tx.senderId in voter_dict and tx.type == 3 and minvote in tx.rawasset:
                    voter_dict[tx.senderId]['status'] = False

            remaining_blocks = len(blocks) - block_nr - 1
            for i in range(remaining_blocks):
                for x in chunk_dict:
                    voter_dict[x]['share'] += chunk_dict[x]
        except IndexError:
            pass

        for i in voter_dict:
            logger.info("{0}  {1}  {2}  {3}  {4}".format(
                i,
                voter_dict[i]['share'],
                voter_dict[i]['status'],
                voter_dict[i]['last_payout'],
                voter_dict[i]['vote_timestamp']))
        return voter_dict, max_timestamp


def get_transactionlist(delegate_pubkey):
    """returns a list of named tuples of all transactions relevant to a specific delegates voters.
    Flow: finds all voters and unvoters, SELECTs all transactions of those voters, names all transactions according to
    the scheme: 'transaction', 'id amount timestamp recipientId senderId rawasset type fee blockId'"""

    res = DbCursor().execute_and_fetchall("""
        SELECT transactions."id", transactions."amount",
               transactions."timestamp", transactions."recipientId",
               transactions."senderId", transactions."rawasset",
               transactions."type", transactions."fee", transactions."blockId"
        FROM transactions
        WHERE transactions."senderId" IN
          (SELECT transactions."recipientId"
           FROM transactions, votes
           WHERE transactions."id" = votes."transactionId"
           AND votes."votes" = '+{0}')
        OR transactions."recipientId" IN
          (SELECT transactions."recipientId"
           FROM transactions, votes
           WHERE transactions."id" = votes."transactionId"
           AND votes."votes" = '+{0}')
        ORDER BY transactions."timestamp" ASC;""".format(delegate_pubkey))

    Transaction = namedtuple(
        'transaction',
        'id amount timestamp recipientId senderId rawasset type fee')
    named_transactions = []

    for i in res:
        tx_id = Transaction(
            id=i[0],
            amount=i[1],
            timestamp=i[2],
            recipientId=i[3],
            senderId=i[4],
            rawasset=i[5],
            type=i[6],
            fee=i[7],
        )

        named_transactions.append(tx_id)
    return named_transactions