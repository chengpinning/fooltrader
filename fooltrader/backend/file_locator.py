# -*- coding: utf-8 -*-

import os
from datetime import datetime

from fooltrader import settings
from fooltrader.utils.time_utils import to_time_str


def get_exchange_dir(security_type='future', exchange='shfe'):
    return os.path.join(settings.FOOLTRADER_STORE_PATH, security_type, exchange)


def get_exchange_trading_calendar_path(security_type='future', exchange='shfe'):
    return os.path.join(get_exchange_dir(security_type, exchange), 'trading_calendar.json')


def get_exchange_cache_dir(security_type='future', exchange='shfe', the_year=None,
                           data_type="day_kdata"):
    if the_year:
        the_dir = os.path.join(settings.FOOLTRADER_STORE_PATH, ".cache", "{}.{}.cache".format(security_type, exchange))
        return os.path.join(the_dir, "{}_{}".format(the_year, data_type))
    return os.path.join(settings.FOOLTRADER_STORE_PATH, ".cache", "{}.{}.cache".format(security_type, exchange))


def get_exchange_cache_path(security_type='future', exchange='shfe', the_date=datetime.today(), data_type="day_kdata"):
    the_dir = get_exchange_cache_dir(security_type=security_type, exchange=exchange, the_year=the_date.year,
                                     data_type=data_type)
    if not os.path.exists(the_dir):
        os.makedirs(the_dir)
    return os.path.join(the_dir, to_time_str(the_time=the_date))


# 标的相关
def get_security_list_path(security_type, exchange):
    return os.path.join(settings.FOOLTRADER_STORE_PATH, security_type, '{}.csv'.format(exchange))


def get_security_dir(security_item=None, security_type=None, exchange=None, code=None):
    if security_type and exchange and code:
        return os.path.join(settings.FOOLTRADER_STORE_PATH, security_type, exchange, code)
    else:
        return os.path.join(settings.FOOLTRADER_STORE_PATH, security_item['type'], security_item['exchange'],
                            security_item['code'])


def get_security_meta_path(security_item=None, security_type=None, exchange=None, code=None):
    return os.path.join(
        get_security_dir(security_item=security_item, security_type=security_type, exchange=exchange, code=code),
        "meta.json")


def get_kdata_dir(security_item, source=None):
    if source == 'sina':
        return os.path.join(get_kdata_dir(security_item), source)

    return os.path.join(get_security_dir(security_item), 'kdata')


def get_kdata_path(security_item, level='day', source=None, year=None, quarter=None):
    if source == 'sina':
        return os.path.join(get_kdata_dir(security_item, source), '{}Q{}.csv'.format(year, quarter))
    return os.path.join(get_kdata_dir(security_item), "{}.csv".format(level))


# tick相关
def get_tick_dir(security_item):
    return os.path.join(settings.FOOLTRADER_STORE_PATH, security_item['type'], security_item['exchange'],
                        security_item['code'], 'tick')


def get_tick_path(item, the_date):
    return os.path.join(get_tick_dir(item), "{}.csv".format(the_date))


# 消息相关
def get_news_dir(item):
    return os.path.join(get_security_dir(item), 'news')


def get_news_path(item, news_type='finance_forecast'):
    return os.path.join(get_news_dir(item), '{}.csv'.format(news_type))


# 财务相关
def get_finance_dir(item):
    return os.path.join(get_security_dir(item), "finance")


def get_balance_sheet_path(item):
    return os.path.join(get_finance_dir(item), "balance_sheet.xls")


def get_income_statement_path(item):
    return os.path.join(get_finance_dir(item), "income_statement.xls")


def get_cash_flow_statement_path(item):
    return os.path.join(get_finance_dir(item), "cash_flow_statement.xls")
# -*- coding: utf-8 -*-
