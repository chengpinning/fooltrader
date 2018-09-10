# -*- coding: utf-8 -*-

import io
import os

import pandas as pd
import scrapy
from scrapy import Request
from scrapy import signals

from fooltrader.backend import file_locator, file_backend
from fooltrader.contract.data_contract import KDATA_STOCK_COL, KDATA_COLUMN_163, KDATA_INDEX_COLUMN_163, \
    KDATA_INDEX_COL
from fooltrader.utils import utils


class StockKdata163Spider(scrapy.Spider):
    name = "stock_kdata_163"

    custom_settings = {
        # 'DOWNLOAD_DELAY': 2,
        # 'CONCURRENT_REQUESTS_PER_DOMAIN': 8,

        'SPIDER_MIDDLEWARES': {
            'fooltrader.middlewares.FoolErrorMiddleware': 1000,
        }
    }

    def start_requests(self):
        security_items = self.settings.get("security_items")

        for security_item in security_items:
            latest_timestamp, _ = file_backend.get_latest_kdata_timestamp(security_item)
            data_path = file_locator.get_kdata_path(security_item)

            if latest_timestamp:
                start = latest_timestamp.strftime('%Y%m%d')
            else:
                start = security_item['listDate'].replace('-', '')

            if security_item['exchange'] == 'sh':
                exchange_flag = 0
            else:
                exchange_flag = 1
            url = self.get_k_data_url(exchange_flag, security_item['code'], start)
            yield Request(url=url, meta={'path': data_path, 'item': security_item},
                          callback=self.download_day_k_data)

    def download_day_k_data(self, response):
        path = response.meta['path']
        item = response.meta['item']

        try:
            # 已经保存的csv数据
            if os.path.exists(path):
                saved_df = pd.read_csv(path, dtype=str)
            else:
                saved_df = pd.DataFrame()

            df = utils.read_csv(io.BytesIO(response.body), encoding='GB2312', na_values='None')
            df['code'] = item['code']
            df['securityId'] = item['id']
            df['name'] = item['name']
            # 指数数据
            if item['type'] == 'index':
                df = df.loc[:,
                     ['日期', 'code', 'name', '最低价', '开盘价', '收盘价', '最高价', '成交量', '成交金额', 'securityId', '前收盘', '涨跌额',
                      '涨跌幅']]
                df['turnoverRate'] = None
                df['tCap'] = None
                df['mCap'] = None
                df['pe'] = None
                df.columns = KDATA_INDEX_COL
            # 股票数据
            else:
                df = df.loc[:,
                     ['日期', 'code', 'name', '最低价', '开盘价', '收盘价', '最高价', '成交量', '成交金额', 'securityId', '前收盘', '涨跌额',
                      '涨跌幅', '换手率', '总市值', '流通市值']]
                df['factor'] = None
                df.columns = KDATA_STOCK_COL

            # 合并到当前csv中
            saved_df = saved_df.append(df, ignore_index=True)

            if item['type'] == 'index':
                saved_df = saved_df.dropna(subset=KDATA_INDEX_COLUMN_163)
                # 保证col顺序
                saved_df = saved_df.loc[:, KDATA_INDEX_COL]
            else:
                saved_df = saved_df.dropna(subset=KDATA_COLUMN_163)
                # 保证col顺序
                saved_df = saved_df.loc[:, KDATA_STOCK_COL]

            saved_df = saved_df.drop_duplicates(subset='timestamp', keep='last')
            saved_df = saved_df.set_index(saved_df['timestamp'], drop=False)
            saved_df.index = pd.to_datetime(saved_df.index)
            saved_df = saved_df.sort_index()
            saved_df.to_csv(path, index=False)
        except Exception as e:
            self.logger.exception('error when getting k data url={} error={}'.format(response.url, e))

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(StockKdata163Spider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider

    def spider_closed(self, spider, reason):
        spider.logger.info('Spider closed: %s,%s\n', spider.name, reason)

    def get_k_data_url(self, exchange, code, start):
        return 'http://quotes.money.163.com/service/chddata.html?code={}{}&start={}'.format(
            exchange, code, start)
