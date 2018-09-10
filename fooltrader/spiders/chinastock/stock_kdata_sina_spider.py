# -*- coding: utf-8 -*-
import logging
import os

import pandas as pd
import scrapy
from scrapy import Request
from scrapy import Selector
from scrapy import signals

from fooltrader.backend import file_locator, file_backend
from fooltrader.backend.file_backend import to_security_item
from fooltrader.consts import DEFAULT_KDATA_HEADER
from fooltrader.contract import data_contract
from fooltrader.utils.time_utils import current_year_quarter

logger = logging.getLogger(__name__)


class StockKDataSinaSpider(scrapy.Spider):
    name = "stock_kdata_sina"

    custom_settings = {
        'DOWNLOAD_DELAY': 2,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 8,

        'SPIDER_MIDDLEWARES': {
            'fooltrader.middlewares.FoolErrorMiddleware': 1000,
        }
    }

    def start_requests(self):
        security_items = self.settings.get("security_items")

        if security_items:
            for security_item in security_items:
                # download the latest quarter kdata at first
                current_year, current_quarter = current_year_quarter()
                the_path = file_locator.get_kdata_path(security_item=security_item, source='sina', year=current_year,
                                                       quarter=current_quarter)

                yield Request(url=self.get_k_data_url(security_item['code'], current_year, current_quarter),
                              headers=DEFAULT_KDATA_HEADER,
                              meta={'latest_quarter': True, 'security_item': security_item, 'the_path': the_path},
                              callback=self.download_day_k_data)

    def download_day_k_data(self, response):
        latest_quarter = response.meta['latest_quarter']
        security_item = response.meta['security_item']
        the_path = response.meta['the_path']
        trs = response.xpath('//*[@id="FundHoldSharesTable"]/tr[position()>1 and position()<=last()]').extract()

        try:
            df = pd.DataFrame(
                columns=data_contract.KDATA_COLUMN_SINA_FQ)

            for idx, tr in enumerate(trs):
                tds = Selector(text=tr).xpath('//td//text()').extract()
                tds = [x.strip() for x in tds if x.strip()]
                securityId = security_item['id']
                timestamp = tds[0]
                open = float(tds[1])
                high = float(tds[2])
                close = float(tds[3])
                low = float(tds[4])
                volume = tds[5]
                turnover = tds[6]
                factor = tds[7]
                df.loc[idx] = [timestamp, security_item['code'], low, open, close, high, volume, turnover, securityId,
                               factor]

            df.to_csv(the_path, index=False)

            if latest_quarter:
                years = response.xpath('//*[@id="con02-4"]/table[1]//select[1]/option/text()').extract()
                for year in years:
                    for quarter in (1, 2, 3, 4):
                        current_year, current_quarter = current_year_quarter()
                        if year == current_year and quarter >= current_quarter:
                            continue
                        the_path = file_locator.get_kdata_path(security_item=security_item, source='sina', year=year,
                                                               quarter=quarter)
                        if os.path.exists(the_path):
                            continue
                        else:
                            url = self.get_k_data_url(item['code'], year, quarter)
                            yield Request(url=url, headers=DEFAULT_KDATA_HEADER,
                                          meta={'path': the_path, 'security_item': item},
                                          callback=self.download_day_k_data)


        except Exception as e:
            self.logger.exception('error when getting k data url={} error={}'.format(response.url, e))

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(StockKDataSinaSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider

    def spider_closed(self, spider, reason):
        spider.logger.info('Spider closed: %s,%s\n', spider.name, reason)

    def get_k_data_url(self, code, year, quarter, fuquan='hfq'):
        if fuquan == 'hfq':
            return 'http://vip.stock.finance.sina.com.cn/corp/go.php/vMS_FuQuanMarketHistory/stockid/{}.phtml?year={}&jidu={}'.format(
                code, year, quarter)
        else:
            return 'http://vip.stock.finance.sina.com.cn/corp/go.php/vMS_MarketHistory/stockid/{}.phtml?year={}&jidu={}'.format(
                code, year, quarter)

    @staticmethod
    def add_factor_to_163(security_item):
        df_163 = file_backend.get_kdata(security_item=security_item)

        if 'factor' in df_163.columns:
            df = df_163[df_163['factor'].isna()]

            if df.empty:
                logger.info("{} 163 factor is ok", security_item['code'])
                return

        sina_dir = file_locator.get_kdata_dir(security_item, source='sina')

        sina_paths = [os.path.join(sina_dir, f) for f in
                      os.listdir(sina_dir)]

        df_sina = pd.DataFrame()
        for sina_path in sina_paths:
            df_sina = file_backend.read_timeseries_df(sina_path)

        df_sina = df_sina[~df_sina.index.duplicated(keep='first')]
        df_163['factor'] = df_sina['factor']

        df_163.to_csv(file_locator.get_kdata_dir(security_item), index=False)


if __name__ == '__main__':
    item = to_security_item('600000', exchange=None)
    StockKDataSinaSpider.add_factor_to_163(item)
