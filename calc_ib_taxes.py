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
        dstr = self.date.strftime('%Y-%m-%d')
        return '<Trade {} {} {} {} per ${}>'.format(dstr, self.kind, self.amount, self.symbol, self.price)

    __repr__ = __str__


def do_the_thing(trades):
    sales = [copy.copy(t) for t in trades if t.kind == 'sell']
    buyings = [copy.copy(t) for t in trades if t.kind == 'buy']
    for s in sales:
        amount_to_find = s.amount
        for b_from in [x for x in buyings if x.symbol == s.symbol]:
            b = copy.copy(b_from)
            b.amount = min(b_from.amount, amount_to_find)
            b_from.amount -= b.amount
            amount_to_find -= b.amount
            s.sold_buyings.append(b)
            if amount_to_find <= 0:
                break
        buyings = [b for b in buyings if b.amount > 0]
        if amount_to_find > 0:
            raise Exception('Not enough buyings to fulfill sale')
    return sales, buyings


usd_rub_exchange_rate_for_date = {}


def get_usd_rub_exchange_rate_for_date(d):
    def d2key(d):
        return d.strftime('%d.%m.%Y')
    if len(usd_rub_exchange_rate_for_date) <= 0:
        with open('usd_rub.dat') as f:
            prev_date = None
            prev_rate = None
            for line in f:
                date_str, rate_str = line.split('\t')
                curr_rate = decimal.Decimal(rate_str.replace(',', '.').replace(' ', ''))
                curr_date = datetime.datetime.strptime(date_str, '%d.%m.%Y')
                if prev_date is not None and (curr_date-prev_date).days > 1:
                    for delta in range((curr_date-prev_date).days - 1):
                        interm_date = prev_date + datetime.timedelta(days=delta+1)
                        usd_rub_exchange_rate_for_date[d2key(interm_date)] = prev_rate
                usd_rub_exchange_rate_for_date[d2key(curr_date)] = curr_rate
                prev_date = curr_date
                prev_rate = curr_rate
    d_str = d2key(d)
    return usd_rub_exchange_rate_for_date[d_str]


def extract_trade_from_ib_scv_annual_activity_report_line(line):
    if not line.startswith('Trades,Data,Order,Stocks,USD,'):
        return None
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
    def to_rub_str(t):
        rate = get_usd_rub_exchange_rate_for_date(t.date)
        return '{:0.2f} RUB ({} * {} * {})'.format(t.amount * t.price * rate, t.amount, t.price, rate)

    trades = load_trades_from_dir('ib_reports')
    print('All trades:')
    for t in trades:
        print(t)
    print()
    sales, buyings_left = do_the_thing(trades)
    print('Sales:')
    for t in sales:
        inc_rub = get_usd_rub_exchange_rate_for_date(t.date) * t.amount * t.price
        print(t)
        print('    Income: {}'.format(to_rub_str(t)))
        print('    What was sold:')
        exp_rub = decimal.Decimal(0)
        for sold in t.sold_buyings:
            print('        *', sold, to_rub_str(sold))
            exp_rub += get_usd_rub_exchange_rate_for_date(sold.date) * sold.amount * sold.price
        print('    Profit: {:0.2f} RUB ({:0.2f} - {:0.2f})'.format(inc_rub-exp_rub, inc_rub, exp_rub))
        print()
    print()
    print('Buyings left:')
    if len(buyings_left) <= 0:
        print('(none)')
    for t in buyings_left:
        print(t)


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
        sales, b_left = do_the_thing(trades)
        print(sales)
        print(b_left)
        self.assertEqual(len(sales), 2)
        self.assertEqual(len(b_left), 1)


    def testRatesDb(self):
        rate = get_usd_rub_exchange_rate_for_date(datetime.datetime(2018, 7, 27, 9, 33, 38))
        self.assertEqual(rate, decimal.Decimal('62.9471'))
        rate = get_usd_rub_exchange_rate_for_date(datetime.datetime(2020, 1, 5, 9, 33, 38))
        self.assertEqual(rate, decimal.Decimal('61.9057'))

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

