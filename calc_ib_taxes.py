#!/usr/bin/env python3


import unittest
import decimal
import datetime
import copy
import csv
import os
import os.path


class Trade:
    def __init__(self, **kw):
        self.date = datetime.datetime.strptime(kw['date'], '%Y-%m-%d, %H:%M:%S')
        self.kind = {'buy': 'buy', 'sell': 'sell'}[kw['kind']]
        self.symbol = kw['symbol']
        self.amount = int(kw['amount'])
        self.price = decimal.Decimal(kw['price'])
        self.sold_buyings = []

    def __str__(self):
        return '<Trade {} {} {} {} {} {}>'.format(self.date, self.kind, self.symbol, self.amount, self.price, self.sold_buyings)

    __repr__ = __str__


def do_the_thing(trades):
    sales = [copy.copy(t) for t in trades if t.kind == 'sell']
    byings = [copy.copy(t) for t in trades if t.kind == 'buy']
    for s in sales:
        amount_to_find = s.amount
        while amount_to_find > 0:
            b_from = byings[0]
            b = copy.copy(b_from)
            if b_from.amount > amount_to_find:
                b.amount = amount_to_find
                b_from.amount -= amount_to_find
            else:
                b.amount = b_from.amount
                byings = byings[1:]
            amount_to_find -= b.amount
            s.sold_buyings.append(b)
    return sales


usd_rub_exchange_rate_for_date = {}


def get_usd_rub_exchange_rate_for_date(d):
    if len(usd_rub_exchange_rate_for_date) <= 0:
        with open('usd_rub.dat') as f:
            for line in f:
                date_str, rate_str = line.split('\t')
                rate = decimal.Decimal(rate_str.replace(',', '.').replace(' ', ''))
                usd_rub_exchange_rate_for_date[date_str.strip()] = rate
    d_str = d.strftime('%d.%m.%Y')
    return usd_rub_exchange_rate_for_date[d_str]


def extract_trade_from_ib_scv_annual_activity_report_line(line):
    if not line.startswith('Trades,Data,Order,Stocks,USD,'):
        return None
    symbol_idx = 5
    date_idx = 6
    amount_idx = 7
    price_idx = 9
    for row in csv.reader([line]):
        t = Trade(
            date=row[6],
            kind = 'sell' if int(row[7]) < 0 else 'buy',
            symbol=row[5],
            amount=abs(int(row[7])),
            price=row[9])
    return t
        

def load_trades_from_dir(dirname):
    trades = []
    for fn in os.listdir(dirname):
        if fn.lower().endswith('.csv'):
            with open(os.path.join(dirname, fn)) as f:
                for line in f:
                    t = extract_trade_from_ib_scv_annual_activity_report_line(line)
                    if t is not None:
                        trades.append(t)
    trades.sort(key=lambda t: t.date)
    return trades


def main():
    print("hi!")


class T(unittest.TestCase):

    def testFifo(self):
        raw = '''2018-11-08, 09:33:38; buy; VOO; 5; 257.72 
        2018-11-30, 10:11:38; buy; VOO; 15; 260.33
        2019-01-15, 10:11:38; sell; VOO; 7; 270.11
        2019-02-01, 10:11:38; sell; VOO; 8; 280.37'''
        trades = []
        for d in [s.strip().split('; ') for s in raw.split('\n')]:
            trade = Trade(date=d[0], kind=d[1], symbol=d[2], amount=d[3], price=d[4])
            trades.append(trade)
        self.assertEqual(len(trades), 4)

        sales = do_the_thing(trades)
        self.assertEqual(len(sales), 2)

        print(sales)

    def testRatesDb(self):
        rate = get_usd_rub_exchange_rate_for_date(datetime.datetime(2018, 7, 27, 9, 33, 38))
        self.assertEqual(rate, decimal.Decimal('62.9471'))

    def testIbReportLine(self):
        line = 'Trades,Data,Order,Stocks,USD,AAPL,"2019-01-03, 09:54:22",1,143.25,142.19,-143.25,-1,144.25,0,-1.06,O'
        t = extract_trade_from_ib_scv_annual_activity_report_line(line)
        self.assertEqual(t.date, datetime.datetime(2019, 1, 3, 9, 54, 22))
        self.assertEqual(t.kind, 'buy')
        self.assertEqual(t.symbol, 'AAPL')
        self.assertEqual(t.amount, 1)
        self.assertEqual(t.price, decimal.Decimal('142.19'))

    def testLoadFromDir(self):
        trades = load_trades_from_dir('test_data')
        self.assertEqual(3, len(trades))




if __name__ == '__main__':
    main()

