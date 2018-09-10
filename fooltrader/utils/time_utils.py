# -*- coding: utf-8 -*-
import datetime

import arrow
import pandas as pd
import tzlocal

CHINA_TZ = 'Asia/Shanghai'

TIME_FORMAT_ISO8601 = "YYYY-MM-DDTHH:mm:ss.SSS"

TIME_FORMAT_DAY = 'YYYY-MM-DD'


# ms(int) or second(float) or str
def to_pd_timestamp(the_time):
    if type(the_time) == int:
        return pd.Timestamp.fromtimestamp(the_time / 1000)

    if type(the_time) == float:
        return pd.Timestamp.fromtimestamp(the_time)

    return pd.Timestamp(the_time)


def to_timestamp(the_time):
    return int(to_pd_timestamp(the_time).tz_localize(tzlocal.get_localzone()).timestamp() * 1000)


def now_timestamp():
    return int(pd.Timestamp.utcnow().timestamp() * 1000)


def now_pd_timestamp():
    return pd.Timestamp.now()


def to_time_str(the_time, fmt=TIME_FORMAT_DAY):
    try:
        return arrow.get(to_pd_timestamp(the_time)).format(fmt)
    except Exception as e:
        return the_time


def next_date(the_time):
    return to_pd_timestamp(the_time) + datetime.timedelta(days=1)


def is_same_date(one, two):
    return to_pd_timestamp(one).date() == to_pd_timestamp(two).date()


def is_same_time(one, two):
    to_timestamp(one) == to_timestamp(two)


def get_year_quarter(time):
    time = to_pd_timestamp(time)
    return time.year, ((time.month - 1) // 3) + 1


def current_year_quarter():
    return get_year_quarter(now_pd_timestamp())


if __name__ == '__main__':
    print(current_year_quarter())
