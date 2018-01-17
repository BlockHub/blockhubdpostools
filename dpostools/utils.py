import time
import datetime


def api_call(func, *args, **kwargs):
    result = None
    for i in range(15):
        if len(args):
            if kwargs:
                result = func(*args, **kwargs)
            else:
                result = func(*args)
        else:
            if kwargs:
                result = func(**kwargs)
            else:
                result = func()
        if result and 'success' in result and result['success']:
            return result
        time.sleep(5)
    return result


def api_call_cached(func, *args, **kwargs):
    if not hasattr(api_call_cached, 'cache'):
        api_call_cached.cache = {}
    cache_key = '%s/%s/%s' % (str(func), str(args), str(kwargs))
    if cache_key not in api_call_cached.cache:
        api_call_cached.cache[cache_key] = api_call(func, *args, **kwargs)
    return api_call_cached.cache[cache_key]


def timestamp(t = None, forfilename=False):
    """Returns a human-readable timestamp given a Unix timestamp 't' or
    for the current time. The Unix timestamp is the number of seconds since
    start of epoch (1970-01-01 00:00:00).
    When forfilename is True, then spaces and semicolons are replace with
    hyphens. The returned string is usable as a (part of a) filename. """

    datetimesep = ' '
    timesep     = ':'
    if forfilename:
        datetimesep = '-'
        timesep     = '-'

    return time.strftime('%Y-%m-%d' + datetimesep +
                         '%H' + timesep + '%M' + timesep + '%S',
                         time.localtime(t))


def arktimestamp(arkt, forfilename=False):
    """Returns a human-readable timestamp given an Ark timestamp 'arct'.
    An Ark timestamp is the number of seconds since Genesis block,
    2017:03:21 15:55:44."""

    t = arkt + time.mktime((2017, 3, 21, 15, 55, 44, 0, 0, 0))
    return '%d %s' % (arkt, timestamp(t))


def arkt_to_datetime(ark_timestamp):
    """convert an ark_timestamp to UTC datetime object"""
    return datetime.datetime(2017, 3, 21, 15, 55, 44) + datetime.timedelta(seconds=ark_timestamp)


def arkt_to_unixt(ark_timestamp):
    """ convert ark timestamp to unix timestamp"""
    res = datetime.datetime(2017, 3, 21, 15, 55, 44) + datetime.timedelta(seconds=ark_timestamp)
    return res.timestamp()


def datetime_to_arkt(datetime):
    """convert a datetime object to ark timestamp"""
    return datetime.timestamp() - datetime.datetime(2017, 3, 21, 15, 55, 44).timestamp


def dictionify(resultset, labelset, single=False, return_list=False):
    if not single and not return_list:
        res = {}
        for i in resultset:
            row = {}
            for a, b in zip(i, labelset):
                row.update({b: a})
            res.update({i[0]: row})
        return res

    if not single and return_list:
        res = []
        for i in resultset:
            row = {}
            for a, b in zip(i, labelset):
                row.update({b: a})
            res.append(row)
        return res

    else:
        res = {}
        for a, b in zip(resultset, labelset):
            res.update({b: a})
        return res