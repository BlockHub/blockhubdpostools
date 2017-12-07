import psycopg2
import psycopg2.extras
from arky import api
from schemes import schemes
from copy import deepcopy
import utils
from utils import dictionify


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


class Blockchain:

    @staticmethod
    def height(network='ark'):
        api.use(network)
        height = []
        for p in utils.api_call(api.Peer.getPeersList)['peers']:

            # some nodes are behind/offline/produce errors and don't respond with a height key
            try:
                height.append(p['height'])
            except Exception:
                pass
        return max(height)


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
        self.columnlabels = deepcopy(self.scheme)
        self.num_delegates = self.scheme['coin_specific_info']['number_of_delegates']

        # removing the table label
        for x in self.columnlabels:
            del self.columnlabels[x]['table']

    def account_details(self, address):
        resultset = self._cursor.execute_and_fetchone(""" 
        SELECT mem."{mem_accounts[address]}",     mem."{mem_accounts[username]}", 
               mem."{mem_accounts[is_delegate]}", mem."{mem_accounts[second_signature]}", 
               mem."{mem_accounts[public_key]}",  mem."{mem_accounts[second_public_key]}", 
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
        blocks."{blocks[height]}", blocks."{blocks[generator_public_key]}"
        FROM {blocks[table]} AS blocks
        ORDER BY blocks."{blocks[height]}" DESC
        LIMIT 1;
        """.format(blocks=self.scheme['blocks']))

        labelset = ['id', 'timestamp', 'height', 'generator_public_key']
        return dictionify(resultset, labelset, single=True)

    def check_node_height(self, max_difference):
        if Blockchain.height() - self.node_height_details()['height'] > max_difference:
            return False
        return True

    def all_delegates(self):
        resultset = self._cursor.execute_and_fetchall("""
        SELECT mem."{mem_accounts[username]}",         mem."{mem_accounts[is_delegate]}",
               mem."{mem_accounts[second_signature]}", mem."{mem_accounts[address]}", 
               mem."{mem_accounts[public_key]}",       mem."{mem_accounts[second_public_key]}", 
               mem."{mem_accounts[balance]}",          mem."{mem_accounts[vote]}", 
               mem."{mem_accounts[rate]}",             mem."{mem_accounts[multi_signatures]}"
        FROM {mem_accounts[table]} AS mem
        WHERE mem."{mem_accounts[is_delegate]}" = 1
        """.format(mem_accounts=self.scheme['mem_accounts']))

        labelset = ['username', 'is_delegate', 'second_signature', 'address', 'public_key', 'second_public_key',
                    'balance', 'vote', 'rate', 'multisignatures', ]

        return dictionify(resultset, labelset)

    def current_delegates(self):
        resultset = self._cursor.execute_and_fetchall("""
        SELECT mem."{mem_accounts[username]}",         mem."{mem_accounts[is_delegate]}",
               mem."{mem_accounts[second_signature]}", mem."{mem_accounts[address]}", 
               mem."{mem_accounts[public_key]}",       mem."{mem_accounts[second_public_key]}", 
               mem."{mem_accounts[balance]}",          mem."{mem_accounts[vote]}", 
               mem."{mem_accounts[rate]}",             mem."{mem_accounts[multi_signatures]}" 
        FROM {mem_accounts[table]} AS mem
        WHERE mem."{mem_accounts[is_delegate]}" = 1
        ORDER BY mem."{mem_accounts[vote]}"
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
               mem."{mem_accounts[public_key]}", blocks."{blocks[height]}"
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

        labelset = ['id', 'amount', 'timestamp', 'recipient_id', 'sender_id',
                    'rawasset', 'type', 'fee', 'username', 'public_key', 'height']

        return dictionify(resultset, labelset)

    def transactions_from_address(self, address):
        resultset = self._cursor.execute_and_fetchall("""
        SELECT trs."{transactions[id]}", trs."{transactions[amount]}",
               trs."{transactions[timestamp]}", trs."{transactions[recipient_id]}",
               trs."{transactions[sender_id]}", trs."{transactions[type]}",
               trs."{transactions[fee]}", blocks."{blocks[height]}"
        FROM {transactions[table]} AS trs, 
             {blocks[table]} AS blocks
        WHERE trs."{transactions[sender_id]}" = '{address}'
        OR trs."{transactions[recipient_id]}" = '{address}'
        AND blocks."{blocks[id]}" = trs."{transactions[block_id]}"
        ORDER BY {blocks[table]}."{blocks[height]}" ASC
        """.format(transactions=self.scheme['transactions'],
                   address=address,
                   blocks=self.scheme['blocks']))

        labelset = ['id', 'amount', 'timestamp', 'recipient_id', 'sender_id', 'type', 'fee', 'height']

        print(dictionify(resultset, labelset))

    def all_votes_by_address(self, address):
        resultset = self._cursor.execute_and_fetchall("""
        SELECT {transactions[table]}."{transactions[timestamp]}", {votes[table]}."{votes[votes]}",
               {mem_accounts[table]}."{mem_accounts[username]}", {mem_accounts[table]}."{mem_accounts[address]}",
               {blocks[table]}."{blocks[height]}", 
               ENCODE({mem_accounts[table]}."{mem_accounts[public_key]}"::BYTEA, 'hex')
        FROM {transactions[table]}, {votes[table]}, {mem_accounts[table]}, {blocks[table]}
        WHERE {transactions[table]}."{transactions[id]}" = {votes[table]}."{votes[transaction_id]}"
        AND {transactions[table]}."{transactions[sender_id]}" = '{address}'
        AND {blocks[table]}."{blocks[id]}" = {transactions[table]}."{transactions[block_id]}"
        AND TRIM(LEADING '+-' FROM {votes[table]}.{votes[votes]}) = ENCODE({mem_accounts[table]}."{mem_accounts[public_key]}"::BYTEA, 'hex')
        ORDER BY {blocks[table]}."{blocks[height]}" ASC;
        """.format(transactions=self.scheme['transactions'],
                   votes=self.scheme['votes'],
                   mem_accounts=self.scheme['mem_accounts'],
                   address=address,
                   blocks=self.scheme['blocks']))

        labelset = ['timestamp', 'vote', 'username', 'address', 'public_key', 'height']

        return dictionify(resultset, labelset)

    def calculate_balance_over_time(self, address):
        resultset = self._cursor.execute_and_fetchall("""
        SELECT {transactions[table]}."{transactions[id]}" as a, 'tx' as b,
        {transactions[table]}."{transactions[amount]}" as c, {transactions[table]}."{transactions[fee]}" as d, 
        {transactions[table]}."{transactions[sender_id]}" as e, {transactions[table]}."{transactions[timestamp]}" as f,
        {blocks[table]}."{blocks[height]}" as g
        FROM {transactions[table]}, {blocks[table]}
        WHERE {transactions[table]}."{transactions[sender_id]}" = '{address}'
        OR {transactions[table]}."{transactions[recipient_id]}" = '{address}'
        AND {blocks[table]}."{blocks[id]}" = {transactions[table]}."{transactions[block_id]}"
        UNION 
        SELECT {blocks[table]}."{blocks[id]}" as a, 'block' as b, {blocks[table]}."{blocks[reward]}" as c, 
        {blocks[table]}."{blocks[total_fee]}" as d, NULL as e, {blocks[table]}."{blocks[timestamp]}" as f, 
        {blocks[table]}."{blocks[height]}" as g
        FROM {blocks[table]}
        WHERE {blocks[table]}."{blocks[generator_public_key]}" = (
          SELECT {mem_accounts[table]}."{mem_accounts[public_key]}"
          FROM {mem_accounts[table]}
          WHERE {mem_accounts[table]}."{mem_accounts[address]}" = '{address}'        )
        ORDER BY g ASC
        """.format(transactions=self.scheme['transactions'],
                   blocks=self.scheme['blocks'],
                   mem_accounts=self.scheme['mem_accounts'],
                   address=address))
        res = {}
        balance = 0

        for i in resultset:
            if i[1] == 'tx':
                if i[4] == address:
                    balance -= (i[2] + i[3])
                    res.update({i[5]: balance})
                else:
                    balance += i[2]
                    res.update({i[5]: balance})

            elif i[1] == 'block':
                balance += i[2] + i[3]
                res.update({i[5]: balance})
        print(res)
        return res

    def get_last_out_transactions(self, address):
        resultset = self._cursor.execute_and_fetchall("""
            SELECT ts."{transactions[recipient_id]}", ts."{transactions[id]}", ts."{transactions[timestamp]}", 
                   ts."{transactions[amount]}", bs.{blocks[height]}
            FROM {transactions[table]} ts, {blocks[table]} bs,
                (SELECT MAX({transactions[table]}."{transactions[timestamp]}") AS max_timestamp, 
                        {transactions[table]}."{transactions[recipient_id]}"
                 FROM {transactions[table]}
                 WHERE {transactions[table]}."{transactions[sender_id]}" = '{address}'
                 GROUP BY {transactions[table]}."{transactions[recipient_id]}") maxresults
            WHERE ts."{transactions[recipient_id]}" = maxresults."{transactions[recipient_id]}"
            AND bs."{blocks[id]}" = ts."{transactions[id]}"

            AND ts."{transactions[timestamp]}"= maxresults.max_timestamp;
                            """.format(transactions=self.scheme['transactions'],
                                       address=address,
                                       blocks=self.scheme['blocks']))

        labelset = ['recipient_id', 'id', 'timestamp', 'amount', 'height']

        return dictionify(resultset, labelset)

    def get_address_all_voters(self, address):
        resultset = self._cursor.execute_and_fetchall("""
            SELECT {transactions[table]}."{transactions[recipient_id]}", 
                   MAX({transactions[table]}."{transactions[timestamp]}"),
                   {blocks[table]}."{blocks[height]}",
                   {transactions[table]}."{transactions[amount]}"
            FROM {transactions[table]}, {transactions[votes]}, {blocks[table]}
            WHERE {transactions[table]}."{transactions[id]}" = {transactions[table]}."{transactions[transaction_id]}"
            AND {blocks[table]}."{blocks[id]}" = {transactions[table]}."{transactions[block_id]}"
            AND {transactions[table]}."{transactions[votes]}" = (
                SELECT {mem_accounts[table]}"{mem_accounts[public_key]}" 
                FROM {mem_accounts[table]}
                WHERE {mem_accounts[table]}"{mem_accounts[address]}" = {address}
            )
            ORDER BY {blocks[table]}."{blocks[height]}" ASC;
               """.format(transactions=self.scheme['transactions'],
                          mem_accounts=self.scheme['mem_accounts'],
                          address=address,
                          blocks=self.scheme['blocks']))

        labelset = ['address', 'timestamp', 'height', 'amount']

        return dictionify(resultset, labelset)

    def get_current_voters_for_address(self, address):
        resultset = self._cursor.execute_and_fetchall("""
            SELECT {transactions[table]}."{transactions[recipient_id]}", AS voter
                   MAX({transactions[table]}."{transactions[timestamp]}"),
                   {blocks[table]}."{blocks[height]}" 
            FROM {transactions[table]}, {transactions[votes]}, {blocks[table]}
            WHERE {transactions[table]}."{transactions[id]}" = {transactions[table]}."{transactions[transaction_id]}"
            AND {blocks[table]}."{blocks[id]}" = {transactions[table]}."{transactions[block_id]}"
            AND {transactions[table]}."{transactions[votes]}" = (
              SELECT {mem_accounts[table]}"{mem_accounts[public_key]}" 
              FROM {mem_accounts[table]}
              WHERE {mem_accounts[table]}"{mem_accounts[address]}" = {address}
            ) 
            WHERE voter IN (
              SELECT {mem_accounts2delegate[table]}."{mem_accounts2delegate[account_id]}"
              FROM {mem_accounts2delegate[table]}
              WHERE {mem_accounts2delegate[table]}."{mem_accounts2delegate[public_key]}" = (
                SELECT {mem_accounts[table]}"{mem_accounts[public_key]}" 
                FROM {mem_accounts[table]}
                WHERE {mem_accounts[table]}"{mem_accounts[address]}" = {address}
              )
            )
            ORDER BY {blocks[table]}."{blocks[height]}" ASC;  
        """.format(transactions=self.scheme['transactions'],
                   mem_accounts=self.scheme['mem_accounts'],
                   mem_accounts2delegate=self.scheme['mem_accounts2delegate'],
                   blocks=self.scheme['blocks'],
                   address=address))

        labelset = ['address', 'timestamp', 'blocks']

        return dictionify(resultset, labelset)


class ArkNode(DposNode):
    def __init__(self, user, password, host='localhost', database='ark_mainnet'):
        DposNode.__init__(self, user=user, password=password, host=host, database=database)

        # change basescheme to Ark
        self.scheme.update(schemes['ark'])
        self.num_delegates = self.scheme['coin_specific_info']['number_of_delegates']
        self.columnlabels.update(schemes['ark'])

        # # removing the table label
        # for x in self.columnlabels:
        #     del self.columnlabels[x]['table']

    def transactions_from_address(self, address):
        resultset = self._cursor.execute_and_fetchall("""
        SELECT {transactions[table]}."{transactions[id]}", {transactions[table]}."{transactions[amount]}",
               {transactions[table]}."{transactions[timestamp]}", {transactions[table]}."{transactions[recipient_id]}",
               {transactions[table]}."{transactions[sender_id]}", {transactions[table]}."{transactions[type]}",
               {transactions[table]}."{transactions[fee]}", {transactions[table]}."{transactions[rawasset]}",
               {transactions[table]}."{transactions[vendor_field]}"
        FROM {transactions[table]}
        WHERE {transactions[table]}."{transactions[sender_id]}" = '{address}'
        OR {transactions[table]}."{transactions[recipient_id]}" = '{address}'
        ORDER BY {transactions[table]}."{transactions[timestamp]}" ASC
        """.format(transactions=self.scheme['transactions'],
                   address=address))


        labelset = ['id', 'amount', 'timestamp', 'recipient_id', 'sender_id', 'type', 'fee', 'rawasset', 'vendor_field']

        return dictionify(resultset, labelset)


class OxycoinNode(DposNode):
    def __init__(self, user, password, host='localhost', database='oxycoin_db_main'):
        DposNode.__init__(self, user=user, password=password, host=host, database=database)

        # change basescheme to Ark
        self.scheme.update(schemes['oxycoin'])
        self.num_delegates = self.scheme['coin_specific_info']['number_of_delegates']
        self.columnlabels.update(schemes['oxycoin'])

        # # removing the table label
        # for x in self.columnlabels:
        #     del self.columnlabels[x]['table']