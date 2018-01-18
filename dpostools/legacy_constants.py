import logging
import logging.handlers
MIN_SEC = 60
HOUR_SEC = MIN_SEC * 60
DAY_SEC = HOUR_SEC * 24
WEEK_SEC = DAY_SEC * 7

# a month is obviously a fucked up concept conceived by the devil,
# so we use 30 day intervals
MONTH_SEC = 30 * DAY_SEC



CONNECTION = {
    'HOST'    : "localhost",
    'DATABASE': "ark_mainnet",
    'USER'    : None,
    'PASSWORD': None,
    }

DELEGATE = {
    'ADDRESS': None,
    'PUBKEY':  None,
    'PASSPHRASE':  None,
}

ARK = 100000000
TX_FEE = 10000000

CALCULATION_SETTINGS = {
    'BLACKLIST': None,
    'EXCEPTIONS': None,
    'MAX': float('inf'),
    'SHARE_FEES': False,
}

SENDER_SETTINGS = {
    'DEFAULT_SHARE': 0,
    'COVER_FEES': False,
    'SHARE_PERCENTAGE_EXCEPTIONS': None,
    'TIMESTAMP_BRACKETS': None,
    'MIN_PAYOUT_DAILY': 0,
    'MIN_PAYOUT_WEEKLY': 0,
    # don't put this at 0, because wallets abandoned wallets will
    # keep accruing a small balance
    'MIN_PAYOUT_MONTHLY': 0,
    # 0 is monday. 6 is sunday
    'DAY_WEEKLY_PAYOUT': 0,
    'DAY_MONTHLY_PAYOUT': 0,
    'PAYOUTSENDER_TEST': True,
    'SENDER_EXCEPTIONS': None,
    'WAIT_TIME_DAY': None,
    'WAIT_TIME_WEEK': None,
    'WAIT_TIME_MONTH': None,
    'SMARTBRIDGE': None
}


LOGGING = {
    'USE': True,
    'LEVEL': logging.WARNING,
    'HANDLER': logging.handlers.RotatingFileHandler('/tmp/arkdbtools.log',
                                                    encoding='utf-8',
                                                    maxBytes=10*1024*1024,
                                                    backupCount=5),

    'FORMAT': '%(asctime)s %(name)-12s %(levelname)-8s %(message)s'
}
