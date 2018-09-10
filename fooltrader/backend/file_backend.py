# -*- coding: utf-8 -*-
import json
import logging
import os
import re

import pandas as pd

from fooltrader.backend import file_locator
from fooltrader.consts import SECURITY_TYPE_MAP_EXCHANGES
from fooltrader.utils.time_utils import to_pd_timestamp, to_time_str, now_pd_timestamp
from fooltrader.utils.utils import get_file_name

logger = logging.getLogger(__name__)


def save_timeseries_df(df, to_path, append=False, drop_duplicate_timestamp=True):
    if drop_duplicate_timestamp:
        df = df.drop_duplicates(subset='timestamp', keep='last')
    df = df.set_index(df['timestamp'], drop=False)
    df.index = pd.to_datetime(df.index)
    df = df.sort_index()

    if append and os.path.exists(to_path):
        with open(to_path, 'a') as f:
            df.to_csv(f, header=False, index=False)
    else:
        df.to_csv(to_path, index=False)


def read_timeseries_df(csv_path, index='timestamp', generate_id=False):
    df = pd.read_csv(csv_path, dtype={"code": str, 'timestamp': str})

    if not df.empty:
        # generate id if need
        if generate_id and 'id' not in df.columns and 'securityId' in df.columns and 'timestamp' in df.columns:
            df['id'] = df['securityId'] + '_' + df['timestamp']

        df = df.set_index(df[index], drop=False)

        if index == 'timestamp' or index == 'reportPeriod':
            df.index = pd.to_datetime(df.index)
            df = df.sort_index()
    return df


def df_filter_date_range(df, start_timestamp=None, end_timestamp=None):
    if start_timestamp:
        df = df[df.index >= to_pd_timestamp(start_timestamp)]
    if end_timestamp:
        df = df[df.index <= to_pd_timestamp(end_timestamp)]
    return df


def get_security_item(security_type, exchanges, code):
    df = get_security_list(security_type=security_type, exchanges=exchanges, codes=[code])

    assert len(df) == 1
    return df.loc[0]


def to_security_item(security_item):
    if type(security_item) == str:
        id_match = re.match(r'(stock|index|future|coin)_([a-z]{2,20})_([a-zA-Z0-9\-]+)',
                            security_item)
        if id_match:
            return get_security_item(security_type=id_match.group(1), exchange=[id_match.group(2)],
                                     code=id_match.group(3))

        # 中国期货
        if re.match(r'^[A-Za-z]{2}\d{4}', security_item):
            return get_security_item(code=security_item, security_type='future', exchanges=['shfe'])

        # 中国股票
        if re.match(r'\d{6}', security_item):
            return get_security_item(code=security_item, security_type='stock', exchanges=['sh', 'sz'])

        # 美国股票
        if re.match(r'[A-Z]{2,20}', security_item):
            return get_security_item(code=security_item, security_type='stock', exchanges=['nasdaq'])
    return security_item


def get_security_list(security_type=None, exchanges=None, codes=None, start_timestamp=None, end_timestamp=None):
    df = pd.DataFrame()
    if type(exchanges) == str:
        exchanges = [exchanges]

    if not exchanges:
        exchanges = SECURITY_TYPE_MAP_EXCHANGES[security_type]

    for exchange in exchanges:
        the_path = file_locator.get_security_list_path(security_type, exchange)
        if os.path.exists(the_path):
            df = df.append(read_timeseries_df(csv_path=the_path))

    if not df.empty:
        df = df_filter_date_range(df, start_timestamp=start_timestamp, end_timestamp=end_timestamp)

        if codes:
            df = df[df["code"].isin(codes)]

    return df


def get_kdata(security_item, start_timestamp=None, end_timestamp=None, the_timestamp=None, level='day',
              generate_id=False):
    security_item = to_security_item(security_item)

    the_path = file_locator.get_kdata_path(security_item, level=level)

    if os.path.isfile(the_path):
        df = read_timeseries_df(csv_path=the_path, generate_id=generate_id)

        if security_item['type'] == 'stock' and 'factor' in df.columns:
            df_kdata_has_factor = df[df['factor'].notna()]
            if df_kdata_has_factor.shape[0] > 0:
                latest_factor = df_kdata_has_factor.tail(1).factor.iat[0]
            else:
                latest_factor = None

        if the_timestamp:
            if the_timestamp in df.index:
                df = df.loc[the_timestamp:the_timestamp, :]
            else:
                return None
        else:
            if start_timestamp or end_timestamp:
                df = df_filter_date_range(df, start_date=start_timestamp, end_date=end_timestamp)

        # 复权处理
        if security_item['type'] == 'stock':
            if 'factor' in df.columns:
                # 后复权是不变的
                df['hfqClose'] = df.close * df.factor
                df['hfqOpen'] = df.open * df.factor
                df['hfqHigh'] = df.high * df.factor
                df['hfqLow'] = df.low * df.factor

                # 前复权需要根据最新的factor往回算,当前价格不变
                if latest_factor:
                    df['qfqClose'] = df.hfqClose / latest_factor
                    df['qfqOpen'] = df.hfqOpen / latest_factor
                    df['qfqHigh'] = df.hfqHigh / latest_factor
                    df['qfqLow'] = df.hfqLow / latest_factor
                else:
                    logger.exception("missing latest factor for {}".format(security_item['id']))

        return df
    return pd.DataFrame()


def get_ticks(security_item, the_date=None, start_date=None, end_date=None):
    security_item = to_security_item(security_item)

    if the_date:
        the_date = to_time_str(the_date)
        tick_path = file_locator.get_tick_path(security_item, the_date)
        yield _parse_tick(tick_path, security_item)
    else:
        tick_dir = file_locator.get_tick_dir(security_item)
        if start_date or end_date:
            if not start_date:
                start_date = security_item['listDate']
                if not start_date:
                    start_date = '1970-01-01'
            if not end_date:
                end_date = now_pd_timestamp()
            tick_paths = [os.path.join(tick_dir, f) for f in
                          os.listdir(tick_dir) if
                          get_file_name(f) in pd.date_range(start=start_date, end=end_date)]
        else:
            tick_paths = [os.path.join(tick_dir, f) for f in
                          os.listdir(tick_dir)]

        for tick_path in sorted(tick_paths):
            yield _parse_tick(tick_path, security_item)


def _parse_tick(tick_path, security_item):
    if os.path.isfile(tick_path):
        df = pd.read_csv(tick_path)
        if security_item['type'] == 'stock':
            df['timestamp'] = get_file_name(tick_path) + " " + df['timestamp']
        else:
            df['timestamp'] = df['timestamp']
        df = df.set_index(df['timestamp'], drop=False)
        df.index = pd.to_datetime(df.index)
        df = df.sort_index()
        df['code'] = security_item['code']
        df['securityId'] = security_item['id']
        return df


def get_available_tick_dates(security_item):
    dir = file_locator.get_tick_dir(security_item)
    return [get_file_name(f) for f in os.listdir(dir)]


def get_latest_kdata_timestamp(security_item, level='day'):
    df = get_kdata(security_item, level=level)
    if df.empty:
        if pd.isna(security_item['listDate']):
            return None, df
        return pd.Timestamp(security_item['listDate']), df

    return df.index[-1], df


def get_latest_tick_timestamp_ids(security_item):
    dates = get_available_tick_dates(security_item)
    if dates:
        dates = sorted(dates)
        tick_path = file_locator.get_tick_path(security_item, dates[-1])
        tick_df = _parse_tick(tick_path, security_item)

        latest_timestamp = tick_df.index[-1]

        if 'id' in tick_df.columns:
            # same time different id
            df = tick_df.loc[latest_timestamp:latest_timestamp, ['id']]
            return latest_timestamp, list(df.values), tick_df
        return latest_timestamp, None, tick_df
    return None, None, None


def get_trading_calendar(security_type='future', exchange='shfe'):
    the_path = file_locator.get_exchange_trading_calendar_path(security_type, exchange)

    trading_dates = []
    if os.path.exists(the_path):
        with open(the_path) as data_file:
            trading_dates = json.load(data_file)
    return trading_dates
