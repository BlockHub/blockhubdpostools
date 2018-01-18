import psycopg2
import psycopg2.extras
from dpostools.schemes import schemes
from dpostools.utils import dictionify
from dpostools import api, exceptions


class DbConnection:
    def __init__(self, user, password, host='localhost', database='ark_mainnet'):
            self._conn = psycopg2.connect(
                host=host,
                database=database,
                user=user,
                password=password
            )

    def connection(self):
        return self._conn


class DbCursor:
    def __init__(self, user, password, host='localhost', database='ark_mainnet', dbconnection=None):
        if not dbconnection:
            dbconnection = DbConnection(
                host=host,
                database=database,
                user=user,
                password=password
            )

        self._cur = dbconnection.connection().cursor()
        self._dict_cur = dbconnection.connection().cursor(cursor_factory=psycopg2.extras.DictCursor)

    def description(self):
        return self._cur.description

    def execute(self, qry, *args, cur_type=None):
        if not cur_type:
            return self._cur.execute(qry, *args)
        elif cur_type == 'dict':
            return self._dict_cur.execute(qry, *args)

    def fetchall(self, cur_type=None):
        if not cur_type:
            return self._cur.fetchall()
        elif cur_type == 'dict':
            self._dict_cur.fetchall()

    def fetchone(self, cur_type=None):
        if not cur_type:
            return self._cur.fetchone()
        elif cur_type == 'dict':
            self._dict_cur.fetchone()

    def execute_and_fetchall(self, qry, *args, cur_type=None):
        self.execute(qry, *args, cur_type=cur_type)
        return self.fetchall(cur_type=cur_type)

    def execute_and_fetchone(self, qry, *args, cur_type=None):
        self.execute(qry, *args, cur_type=cur_type)
        return self.fetchone(cur_type=cur_type)

class DposNode:
    def __init__(self, user, password, host='localhost', database='ark_mainnet', ):
        self._cursor = DbCursor(
            user=user,
            password=password,
            host=host,
            database=database
        )

        # set generic scheme
        self.scheme = schemes['base']
        self.num_delegates = self.scheme['coin_specific_info']['number_of_delegates']
        self.network = 'ark'

    def account_details(self, address):
        resultset = self._cursor.execute_and_fetchone(""" 
        SELECT mem."{mem_accounts[address]}",     mem."{mem_accounts[username]}", 
               mem."{mem_accounts[is_delegate]}", mem."{mem_accounts[second_signature]}", 
               ENCODE(mem."{mem_accounts[public_key]}"::BYTEA, 'hex'),  ENCODE(mem."{mem_accounts[second_public_key]}"::BYTEA, 'hex'), 
               mem."{mem_accounts[balance]}",     mem."{mem_accounts[vote]}", 
               mem."{mem_accounts[rate]}",        mem."{mem_accounts[multi_signatures]}"
        FROM {mem_accounts[table]} as mem
        WHERE mem."{mem_accounts[address]}" = '{address}';
        """.format(
            mem_accounts=self.scheme['mem_accounts'],
            address=address))

        labelset = ['address', 'username', 'is_delegate', 'second_signature', 'public_key', 'second_public_key',
                    'balance', 'vote', 'rate', 'multisignatures']

        return dictionify(resultset, labelset, single=True,)

    def node_height_details(self):
        resultset = self._cursor.execute_and_fetchone("""
        SELECT blocks."{blocks[id]}", blocks."{blocks[timestamp]}",
        blocks."{blocks[height]}", ENCODE(blocks."{blocks[generator_public_key]}"::BYTEA, 'hex')
        FROM {blocks[table]} AS blocks
        ORDER BY blocks."{blocks[height]}" DESC
        LIMIT 1;
        """.format(blocks=self.scheme['blocks']))

        labelset = ['block_id', 'timestamp', 'height', 'generator_public_key']
        return dictionify(resultset, labelset, single=True)

    def check_node_height(self, max_difference):
        if api.Network(network=self.network).status()['network_height'] - self.node_height_details()['height'] > max_difference:
            return False
        return True

    def all_delegates(self):
        resultset = self._cursor.execute_and_fetchall("""
        SELECT
        mem."{mem_accounts[address]}", 
        mem."{mem_accounts[username]}",         
        mem."{mem_accounts[is_delegate]}",
        mem."{mem_accounts[second_signature]}", 
        ENCODE(mem."{mem_accounts[public_key]}"::BYTEA, 'hex'), 
        ENCODE(mem."{mem_accounts[second_public_key]}"::BYTEA, 'hex'),
        mem."{mem_accounts[balance]}",          
        mem."{mem_accounts[vote]}", 
        mem."{mem_accounts[rate]}",             
        mem."{mem_accounts[multi_signatures]}"
        FROM {mem_accounts[table]} AS mem
        WHERE mem."{mem_accounts[is_delegate]}" = 1
        """.format(mem_accounts=self.scheme['mem_accounts']))

        labelset = ['address', 'username', 'is_delegate', 'second_signature', 'public_key', 'second_public_key',
                    'balance', 'vote', 'rate', 'multisignatures', ]

        return dictionify(resultset, labelset)

    def current_delegates(self):
        resultset = self._cursor.execute_and_fetchall("""
        SELECT mem."{mem_accounts[username]}",         mem."{mem_accounts[is_delegate]}",
               mem."{mem_accounts[second_signature]}", mem."{mem_accounts[address]}", 
               ENCODE(mem."{mem_accounts[public_key]}"::BYTEA, 'hex'), ENCODE(mem."{mem_accounts[second_public_key]}"::BYTEA, 'hex'),
               mem."{mem_accounts[balance]}",          mem."{mem_accounts[vote]}", 
               mem."{mem_accounts[rate]}",             mem."{mem_accounts[multi_signatures]}" 
        FROM {mem_accounts[table]} AS mem
        WHERE mem."{mem_accounts[is_delegate]}" = 1
        ORDER BY mem."{mem_accounts[vote]}" DESC
        LIMIT {num_delegates}
        """.format(mem_accounts=self.scheme['mem_accounts'],
                   num_delegates=self.num_delegates))

        labelset = ['username', 'is_delegate', 'second_signature', 'address', 'public_key', 'second_public_key',
                    'balance', 'vote', 'rate', 'multisignatures', ]

        return dictionify(resultset, labelset)

    def payouts_to_address(self, address):
        resultset = self._cursor.execute_and_fetchall("""
            SELECT DISTINCT trs."{transactions[id]}", trs."{transactions[amount]}",
                   trs."{transactions[timestamp]}", trs."{transactions[recipient_id]}",
                   trs."{transactions[sender_id]}", trs."{transactions[type]}", 
                   trs."{transactions[fee]}", mem."{mem_accounts[username]}", 
                   ENCODE(mem."{mem_accounts[public_key]}"::BYTEA, 'hex'), blocks."{blocks[height]}"
            FROM {mem_accounts[table]} mem   
              INNER JOIN {transactions[table]} trs 
              ON 
              (trs."{transuactions[sender_id]}"=mem."{mem_accounts[address]}")
              INNER JOIN {blocks[table]} blocks
              ON (blocks."{blocks[id]}" = trs."{transactions[block_id]}")
            WHERE trs."{transactions[recipient_id]}" = '{address}'
            AND mem."{mem_accounts[is_delegate]}" = 1 
            ORDER BY blocks."{blocks[height]}" ASC
            """.format(
                transactions=self.scheme['transactions'],
                mem_accounts=self.scheme['mem_accounts'],
                address=address,
                blocks=self.scheme['blocks']))

        labelset = ['tx_id', 'amount', 'timestamp', 'recipient_id', 'sender_id',
                    'type', 'fee', 'username', 'public_key', 'height']

        return dictionify(resultset, labelset)

    def transactions_from_address(self, address):
        resultset = self._cursor.execute_and_fetchall("""
            SELECT 
            trs."{transactions[id]}" AS id, 
            trs."{transactions[amount]}",
            trs."{transactions[timestamp]}", 
            trs."{transactions[recipient_id]}",
            trs."{transactions[sender_id]}", 
            trs."{transactions[type]}",
            trs."{transactions[fee]}", 
            blocks."{blocks[height]}"
            FROM 
            {transactions[table]} AS trs
              INNER JOIN {blocks[table]} AS blocks
                 ON (blocks."{blocks[id]}" = trs."{transactions[block_id]}") 
            WHERE 
            trs."{transactions[sender_id]}" = '{address}'
            OR 
            trs."{transactions[recipient_id]}" = '{address}'
            ORDER BY 
            blocks."{blocks[height]}" ASC
            """.format(transactions=self.scheme['transactions'],
                       address=address,
                       blocks=self.scheme['blocks']))

        labelset = ['tx_id', 'amount', 'timestamp', 'recipient_id', 'sender_id', 'type', 'fee', 'height']

        return dictionify(resultset, labelset)

    def all_votes_by_address(self, address):
        resultset = self._cursor.execute_and_fetchall("""
            SELECT 
            trs."{transactions[timestamp]}", 
            votes."{votes[votes]}",
            mem."{mem_accounts[username]}", 
            mem."{mem_accounts[address]}", 
            ENCODE(mem."{mem_accounts[public_key]}"::BYTEA, 'hex'),
            blocks."{blocks[height]}"
            FROM {transactions[table]} AS trs 
                 INNER JOIN {blocks[table]} blocks
                 ON (blocks."{blocks[id]}" = trs."{transactions[block_id]}")
                 INNER JOIN {votes[table]} AS votes
                 ON (trs."{transactions[id]}" = votes."{votes[transaction_id]}")
                 INNER JOIN {mem_accounts[table]} mem
                 ON (TRIM(LEADING '+-' FROM votes."{votes[votes]}") = ENCODE(mem."{mem_accounts[public_key]}"::BYTEA, 'hex'))
            WHERE trs."{transactions[sender_id]}" = '{address}'
        
            ORDER BY blocks."{blocks[height]}" ASC;
            """.format(transactions=self.scheme['transactions'],
                       votes=self.scheme['votes'],
                       mem_accounts=self.scheme['mem_accounts'],
                       address=address,
                       blocks=self.scheme['blocks']))

        labelset = ['timestamp', 'vote', 'username', 'address', 'public_key', 'height']

        return dictionify(resultset, labelset)

    def calculate_balance_over_time(self, address):
        # todo check if balance over time results in a correct balance for delegates

        qry = """
        SELECT * FROM (
        SELECT 
        'tx' AS a,
        trs."{transactions[id]}" AS b,
        trs."{transactions[amount]}" AS c, 
        trs."{transactions[fee]}" AS d, 
        trs."{transactions[sender_id]}" AS e, 
        trs."{transactions[timestamp]}" AS f,
        blocks."{blocks[height]}" AS g
        FROM {transactions[table]} AS trs 
             INNER JOIN {blocks[table]} blocks
             ON (blocks."{blocks[id]}" = trs."{transactions[block_id]}")
        WHERE trs."{transactions[sender_id]}" = '{address}'
        OR trs."{transactions[recipient_id]}" = '{address}'
        UNION
        SELECT
        'block' AS a, 
        blocks."{blocks[id]}" AS b, 
        blocks."{blocks[reward]}" AS c,
        blocks."{blocks[total_fee]}" AS d,
        NULL AS e,
        blocks."{blocks[timestamp]}" AS f,
        blocks."{blocks[height]}" AS g
        FROM blocks 
        WHERE
        blocks."{blocks[generator_public_key]}" = (
                    SELECT mem2."{mem_accounts[public_key]}"
                    FROM {mem_accounts[table]} mem2
                    WHERE mem2."{mem_accounts[address]}" = '{address}')) total
        
        ORDER BY total.g ASC
        """.format(transactions=self.scheme['transactions'],
                   blocks=self.scheme['blocks'],
                   address=address,
                   mem_accounts=self.scheme['mem_accounts'],
                   )
        resultset = self._cursor.execute_and_fetchall(qry)
        res = {}
        balance = 0
        for i in resultset:
            if i[0] == 'tx':
                if i[4] == address:
                    balance -= (i[2] + i[3])
                else:
                    balance += i[2]

            elif i[0] == 'block':
                balance += i[2] + i[3]
            if balance < 0:
                raise exceptions.NegativeBalanceError

            res.update({i[5]: balance})
        return res

    def get_last_out_transactions(self, address):
        resultset = self._cursor.execute_and_fetchall("""
            SELECT
            trs."{transactions[recipient_id]}",
            trs."{transactions[timestamp]}",
            blocks."{blocks[height]}",
            trs."{transactions[id]}", 
            trs."{transactions[amount]}",
            trs."{transactions[fee]}"
            FROM {transactions[table]} trs
              INNER JOIN {blocks[table]} blocks
              ON (blocks."{blocks[id]}" = trs."{transactions[block_id]}"),
            (SELECT 
             MAX(ts."{transactions[timestamp]}") AS max_timestamp, 
             ts."{transactions[recipient_id]}"
             FROM {transactions[table]} AS ts
             WHERE ts."{transactions[sender_id]}" = '{address}'
             GROUP BY ts."{transactions[recipient_id]}") AS maxresults
            
            WHERE trs."recipientId" = maxresults."recipientId"
            AND trs."timestamp"= maxresults.max_timestamp;
            """.format(transactions=self.scheme['transactions'],
                       address=address,
                       blocks=self.scheme['blocks']))

        labelset = ['recipient_id', 'timestamp', 'height', 'tx_id', 'amount', 'fee']

        return dictionify(resultset, labelset)

    def get_historic_voters(self, address):
        delegate_public_key = self.account_details(address=address)['public_key']
        plusvote = '{{"votes":["+{0}"]}}'.format(delegate_public_key)

        resultset = self._cursor.execute_and_fetchall("""
            SELECT 
            trs."{transactions[sender_id]}", 
            trs."{transactions[timestamp]}",
            trs."{transactions[id]}",
            blocks."{blocks[height]}"
            FROM 
            {transactions[table]} AS trs
            INNER JOIN {blocks[table]} AS blocks
                ON (blocks."{blocks[id]}" = trs."{transactions[block_id]}")
            WHERE 
            trs."{transactions[rawasset]}" = '{plusvote}'
            ORDER BY {blocks[table]}."{blocks[height]}" ASC;
               """.format(transactions=self.scheme['transactions'],
                          mem_accounts=self.scheme['mem_accounts'],
                          address=address,
                          plusvote=plusvote,
                          blocks=self.scheme['blocks']))

        labelset = ['address', 'timestamp', 'id', 'height']

        return dictionify(resultset, labelset)

    def get_current_voters(self, address):
        resultset = self._cursor.execute_and_fetchall("""
            SELECT 
            trs."{transactions[sender_id]}",
            trs."{transactions[timestamp]}",
            trs."{transactions[id]}",
            blocks."{blocks[height]}" AS block,
            trs."{transactions[id]}"
            FROM
            {transactions[table]} AS trs
               INNER JOIN {blocks[table]} blocks
                 ON (blocks."{blocks[id]}" = trs."{transactions[block_id]}")
            WHERE
            trs."{transactions[type]}" = 3
            AND
            trs."{transactions[sender_id]}" IN (
                 
                 SELECT mem."{mem_accounts2delegates[account_id]}"
                 FROM
                 {mem_accounts2delegates[table]} AS mem
                 WHERE
                 mem."{mem_accounts2delegates[dependent_id]}" = (
                 SELECT ENCODE(mem2."{mem_accounts[public_key]}"::BYTEA, 'hex')
                        FROM {mem_accounts[table]} AS mem2
                        WHERE mem2."{mem_accounts[address]}" = '{address}'           
              )) 
            ORDER BY block DESC
            """.format(
                transactions=self.scheme['transactions'],
                blocks=self.scheme['blocks'],
                mem_accounts2delegates=self.scheme['mem_accounts2delegates'],
                mem_accounts=self.scheme['mem_accounts'],
                address=address
            ))

        labelset = ['address', 'timestamp', 'id', 'height']

        return dictionify(resultset, labelset)

    def get_blocks(self, delegate_address):
        resultset = self._cursor.execute_and_fetchall("""
             SELECT 
             blocks."{blocks[height]}", 
             blocks."{blocks[timestamp]}", 
             blocks."{blocks[id]}", 
             blocks."{blocks[total_fee]}", 
             blocks."{blocks[reward]}"
             FROM 
             {blocks[table]} AS blocks
             WHERE 
             blocks."{blocks[generator_public_key]}" = (
                            SELECT mem."{mem_accounts[public_key]}"
                            FROM {mem_accounts[table]} AS mem
                            WHERE mem."{mem_accounts[address]}" = '{address}')
             ORDER BY 
             blocks."{blocks[height]}" ASC
             """.format(blocks=self.scheme['blocks'],
                        mem_accounts=self.scheme['mem_accounts'],
                        address=delegate_address))

        labelset = ['height', 'timestamp', 'id', 'total_fee', 'reward']

        return dictionify(resultset, labelset)

    def get_events_vote_cluster(self, delegate_address):
        ''' Returns all transactions and forged blocks by voters clustered around a single delegate_address'''

        delegate_pubkey = self.account_details(address=delegate_address)['public_key']

        plusvote = '+{delegate_pubkey}'.format(delegate_pubkey=delegate_pubkey)

        resultset = self._cursor.execute_and_fetchall("""
            SELECT *
             FROM (
            SELECT 
            trs."{transactions[id]}" AS a,
            'transaction' AS b, 
            trs."{transactions[amount]}" AS c,
            trs."{transactions[timestamp]}" AS d, 
            trs."{transactions[recipient_id]}" AS e,
            trs."{transactions[sender_id]}" AS f, 
            trs."{transactions[rawasset]}" AS g,
            trs."{transactions[type]}" AS h, 
            trs."{transactions[fee]}" AS i, 
            trs."{transactions[block_id]}" AS j,
            blocks."{blocks[height]}" AS k
            FROM {transactions[table]} AS trs
            INNER JOIN {blocks[table]} AS blocks
                 ON (blocks."{blocks[id]}" = trs."{transactions[block_id]}")
            WHERE trs."{transactions[sender_id]}" IN
              (SELECT trs."{transactions[sender_id]}"
               FROM {transactions[table]} AS trs, {votes[table]} AS votes
               WHERE trs."{transactions[id]}" = votes."{votes[transaction_id]}"
               AND votes."{votes[votes]}" = '{plusvote}') 
            OR trs."{transactions[recipient_id]}" IN
              (SELECT trs."{transactions[sender_id]}"
               FROM {transactions[table]} AS trs, {votes[table]} AS votes
               WHERE trs."{transactions[id]}" = votes."{votes[transaction_id]}"
               AND votes."{votes[votes]}" = '{plusvote}') 
            UNION
            SELECT 
            blocks."{blocks[id]}" AS a, 
            'block' AS b, 
            blocks."{blocks[reward]}"as c, 
            blocks."{blocks[total_fee]}" AS d,
            ENCODE(mem."{mem_accounts[public_key]}"::BYTEA, 'hex') AS e,
            mem."{mem_accounts[address]}" AS f,
            mem."{mem_accounts[username]}" AS g,
            NULL AS h,
            blocks."{blocks[timestamp]}" AS i,
            NULL AS j,
            blocks."{blocks[height]}" AS k
            FROM blocks
              INNER JOIN {mem_accounts[table]} AS mem
              ON (mem."{mem_accounts[public_key]}" = blocks."{blocks[generator_public_key]}")  
            WHERE
            blocks."{blocks[generator_public_key]}" IN (
                    SELECT mem2."{mem_accounts[public_key]}"
                    FROM {mem_accounts[table]} mem2
                    WHERE mem2."{mem_accounts[address]}" IN 
                    (SELECT trs."{transactions[sender_id]}"
                     FROM {transactions[table]} AS trs, {votes[table]} AS votes
                     WHERE trs."{transactions[id]}" = votes."{votes[transaction_id]}"
                     AND votes."{votes[votes]}" = '{plusvote}') 
               )) total
               
            ORDER BY total.k ASC;""".format(
                address=delegate_address,
                transactions=self.scheme['transactions'],
                blocks=self.scheme['blocks'],
                mem_accounts=self.scheme['mem_accounts'],
                mem_accounts2delegates=self.scheme['mem_accounts2delegates'],
                votes=self.scheme['votes'],
                plusvote=plusvote))
        res = {}
        for i in resultset:

            if i[1] == 'transaction':
                res.update({i[0]: {
                   'tx_id': i[0],
                   'event_type': i[1],
                   'amount': i[2],
                   'timestamp': i[3],
                   'recipient_id': i[4],
                   'sender_id': i[5],
                   'rawasset': i[6],
                   'type': i[7],
                   'fee': i[8],
                   'block_id': i[9],
                   'height': i[10]
                }})

            elif i[1] == 'block':
                res.update({i[0]: {
                    'block_id': i[0],
                    'event_type': i[1],
                    'reward': i[2],
                    'total_fee': i[3],
                    'timestamp': i[8],
                    'address': i[5],
                    'username': i[6],
                    'public_key': i[4],
                    'height': i[10]
                }})

        return res

    def tbw(self, delegate_address, blacklist=None, share_fees=False, compound_interest=False):
        """This function doesn't work yet. Instead use legacy.trueshare() for a functional tbw script"""
        if not blacklist:
            blacklist = []

        delegate_public_key = self.account_details(address=delegate_address)['public_key']
        height_at_calculation = self.node_height_details()['height']

        # string format of the rawasset
        minvote = '{{"votes":["-{0}"]}}'.format(delegate_public_key)
        plusvote = '{{"votes":["+{0}"]}}'.format(delegate_public_key)

        events = self.get_events_vote_cluster(delegate_address)
        votes = self.get_historic_voters(delegate_address)
        blocks = self.get_blocks(delegate_address)
    
        # create a map of voters
        voter_dict = {}
        for voter in votes:
            voter_dict.update({voter: {
                'balance': 0.0,
                'status': False,
                'last_payout': votes[voter]['height'],
                'share': 0.0,
                'vote_height': votes[voter]['height'],
                'blocks_forged': []}
            })

        for blacklisted_address in blacklist:
            voter_dict.pop(blacklisted_address, None)

        last_payout = self.get_last_out_transactions(delegate_address)

        # not all voters have had a payout, thus a KeyError is thrown
        for payout in last_payout:
            try:
                voter_dict[payout]['last_payout'] = last_payout[payout]['height']
            except KeyError:
                pass

        # the change in the previous state of the voter_dict. This is added to the voterdict if
        # no state change occurs in the blockchain.
        delta_state = {}
        no_state_change = False

        block_keys = sorted(list(blocks.keys()))
        block_nr = 0

        try:
            for id in events:
                # calculating poolbalances and updating shares
                if events[id]['height'] > blocks[block_keys[block_nr]]['height']:

                    # if the state is the same for the votepool, the previous calculation can be reused.
                    block_nr += 1
                    if no_state_change:
                        for x in delta_state:
                            voter_dict[x]['share'] += delta_state[x]
                        continue


                    # update pool balances
                    poolbalance = 0
                    delta_state = {}

                    for i in voter_dict:

                        # here we update the poolbalance
                        if compound_interest:
                            balance = voter_dict[i]['balance'] + voter_dict[i]['share']
                        else:
                            balance = voter_dict[i]['balance']

                        if voter_dict[i]['status']:
                            # if not voter_dict[i]['balance'] < 0:
                            poolbalance += balance
                            # else:
                            #     raise exceptions.NegativeBalanceError('balance lower than zero for: {0}. balance: {1}'.format(i, voter_dict[i]['balance']))

                    # here we calculate the share per voter
                    for i in voter_dict:
                        if compound_interest:
                            balance = voter_dict[i]['balance'] + voter_dict[i]['share']
                        else:
                            balance = voter_dict[i]['balance']

                        if voter_dict[i]['status'] and voter_dict[i]['last_payout'] < blocks[block_keys[block_nr]]['height']:
                            if share_fees:
                                share = (balance / poolbalance) * (blocks[block_keys[block_nr]]['reward'] +
                                                                   blocks[block_keys[block_nr]]['totalFee'])
                            else:
                                share = (balance / poolbalance) * blocks[block_keys[block_nr]]['reward']
                            voter_dict[i]['share'] += share
                            delta_state.update({i: share})

                    no_state_change = True
                    continue

                # parsing an event
                no_state_change = False

                if events[id]['event_type'] == 'transaction':
                    if events[id]['recipient_id'] == 'Acw2vAVA48TcV8EnoBmZKJdV8bxnW6Y4E9':
                        print(events[id]['amount'])

                # parsing a transaction
                if events[id]['event_type'] == 'transaction':
                    if events[id]['recipient_id'] in voter_dict:
                        voter_dict[events[id]['recipient_id']]['balance'] += events[id]['amount']

                    if events[id]['sender_id'] in voter_dict:
                        voter_dict[events[id]['sender_id']]['balance'] -= (events[id]['amount'] + events[id]['fee'])

                    if events[id]['sender_id'] in voter_dict and events[id]['type'] == 3 and plusvote in events[id]['rawasset']:
                        voter_dict[events[id]['sender_id']]['status'] = True

                    if events[id]['sender_id'] in voter_dict and events[id]['type'] == 3 and minvote in events[id]['rawasset']:
                        voter_dict[events[id]['sender_id']]['status'] = False

                # parsing a forged block (if forged by a voter)
                if events[id]['event_type'] == 'block':
                    voter_dict[events[id]['address']]['balance'] += (events[id]['reward'] + events[id]['total_fee'])

            # the transaction for loop ends with the final transaction. However more blocks may be forged. This copies
            # the final delta share and adds it to the share x the amount of blocks left.
            remaining_blocks = len(block_keys) - block_nr - 1
            for i in range(remaining_blocks):
                for x in delta_state:
                    voter_dict[x]['share'] += delta_state[x]

            # and indexerror indicates that we have ran out of forged blocks, thus the calculation is done (blocks[block_nr]
            # throw the error)
        except IndexError:
            raise

        return voter_dict, height_at_calculation


class ArkNode(DposNode):
    def __init__(self, user, password, host='localhost', database='ark_mainnet'):
        DposNode.__init__(self, user=user, password=password, host=host, database=database)

        # change basescheme to Ark
        self.scheme.update(schemes['ark'])
        self.num_delegates = self.scheme['coin_specific_info']['number_of_delegates']


        # # removing the table label
        # for x in self.columnlabels:
        #     del self.columnlabels[x]['table']

    def payouts_to_address(self, address):
        resultset = self._cursor.execute_and_fetchall("""
        SELECT DISTINCT 
        trs."{transactions[id]}", 
        trs."{transactions[amount]}",
        trs."{transactions[timestamp]}", 
        trs."{transactions[recipient_id]}",
        trs."{transactions[sender_id]}", 
        trs."{transactions[type]}", 
        trs."{transactions[fee]}", 
        mem."{mem_accounts[username]}", 
        ENCODE(mem."{mem_accounts[public_key]}"::BYTEA, 'hex'), 
        blocks."{blocks[height]}",
        trs."{transactions[vendor_field]}"
        FROM {mem_accounts[table]} mem   
          INNER JOIN {transactions[table]} trs 
          ON 
          (trs."{transactions[sender_id]}"=mem."{mem_accounts[address]}")
          INNER JOIN {blocks[table]} blocks
          ON (blocks."{blocks[id]}" = trs."{transactions[block_id]}")
        WHERE trs."{transactions[recipient_id]}" = '{address}'
        AND mem."{mem_accounts[is_delegate]}" = 1 
        ORDER BY blocks."{blocks[height]}" ASC
        """.format(
            transactions=self.scheme['transactions'],
            mem_accounts=self.scheme['mem_accounts'],
            address=address,
            blocks=self.scheme['blocks']))

        labelset = ['tx_id', 'amount', 'timestamp', 'recipient_id', 'sender_id',
                    'type', 'fee', 'username', 'public_key', 'height', 'vendor_field']
        return dictionify(resultset, labelset)

    def transactions_from_address(self, address):
        resultset = self._cursor.execute_and_fetchall("""
        SELECT 
        trs."{transactions[id]}" AS tx_id, 
        trs."{transactions[amount]}",
        trs."{transactions[timestamp]}", 
        trs."{transactions[recipient_id]}",
        trs."{transactions[sender_id]}", 
        trs."{transactions[type]}",
        trs."{transactions[fee]}",
        trs."{transactions[rawasset]}",
        blocks."{blocks[height]}",
        trs."{transactions[vendor_field]}" 
        FROM 
        {transactions[table]} AS trs
            INNER JOIN {blocks[table]} AS blocks
                     ON (blocks."{blocks[id]}" = trs."{transactions[block_id]}")
        WHERE 
        trs."{transactions[sender_id]}" = '{address}'
        OR 
        trs."{transactions[recipient_id]}" = '{address}' 
        ORDER BY 
        blocks."{blocks[height]}" ASC
        """.format(transactions=self.scheme['transactions'],
                   address=address,
                   blocks=self.scheme['blocks']))

        labelset = ['tx_id', 'amount', 'timestamp', 'recipient_id', 'sender_id', 'type', 'fee', 'rawasset', 'height', 'vendor_field']

        return dictionify(resultset, labelset)


class OxycoinNode(DposNode):
    def __init__(self, user, password, host='localhost', database='oxycoin_db_main'):
        DposNode.__init__(self, user=user, password=password, host=host, database=database)

        # change basescheme to Ark
        self.scheme.update(schemes['oxycoin'])
        self.num_delegates = self.scheme['coin_specific_info']['number_of_delegates']
        self.network = 'oxy'
