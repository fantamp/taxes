#!/usr/bin/env python3

import unittest
import decimal
import datetime
import copy
import csv
import os
import os.path


# Where to get currency exchange rate:
# https://cbr.ru/currency_base/dynamics/?UniDbQuery.Posted=True&UniDbQuery.mode=1&UniDbQuery.date_req1=&UniDbQuery.date_req2=&UniDbQuery.VAL_NM_RQ=R01235&UniDbQuery.From=01.01.2019&UniDbQuery.To=31.12.2019
#
# See also:
# similar IB tax calculation program: https://github.com/manushkin/tax_ib

# Some docs
# - https://journal-tinkoff-ru.turbopages.org/s/journal.tinkoff.ru/ask/securities-taxes/


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
        return '{} {} {} {} {}'.format(dstr, self.kind, self.amount, self.symbol, m(self.price))
        #return '<Trade {} {} {} {} for ${} per unit>'.format(dstr, self.kind, self.amount, self.symbol, self.price)

    __repr__ = __str__


class MoneyInOut:
    def __init__(self, rec, reason):
        self.reason = reason
        self.date = datetime.datetime.strptime(rec['Date'], '%Y-%m-%d') 
        self.key = rec['Description']
        self.symbol = self.key.partition('(')[0].strip()
        self.amount = decimal.Decimal(rec['Amount'])
        self.withholdings = []

    def __str__(self):
        dstr = self.date.strftime('%d.%m.%Y')
        return '<{} {} {} {}>'.format(self.reason, dstr, self.symbol, m(self.amount))


def m(x):
    return '${:0.2f}'.format(x) if x >= 0 else '-${:0.2f}'.format(x * -1)


def read_report(f):
    table_name, records, tables = (None, None, {})
    def flush():
        if table_name is not None:
            tables[table_name] = records
    for row in csv.reader(f):
        if len(row) >= 2 and row[1] == 'Header':
            flush()
            table_name, records, keys = (row[0], [], row)
        elif row[1] == 'Data':
            rec = dict(zip(keys, row))
            if table_name in ('Dividends', 'Withholding Tax') and rec['Currency'] == 'Total':
                continue
            records.append(rec)
    flush()
    return tables


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
            raise Exception('Not enough buyings to fulfill sale: {}'.format(s))
    return sales, buyings


def calc_divs(raw_divs, withholdings):
    res = []
    for d1 in raw_divs:
        d2 = copy.copy(d1)
        d2.withholdings = [w for w in withholdings if w.symbol == d2.symbol and w.date == d2.date]
        res.append(d2)
    return res


usd_rub_exchange_rate_for_date = {}


def get_usd_rub_exchange_rate_for_date(d):
    def d2key(d):
        return d.strftime('%d.%m.%Y')
    if len(usd_rub_exchange_rate_for_date) <= 0:
        with open('data/usd_rub.dat') as f:
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


def trade_from_report_rec(rec):
    q = int(rec['Quantity'])
    t = Trade(
        date=rec['Date/Time'],
        kind = 'sell' if q < 0 else 'buy',
        symbol=rec['Symbol'],
        amount=abs(q),
        price=rec['T. Price'])
    if t.symbol == 'SGOL' and t.date < datetime.datetime.strptime('2019-12-24', '%Y-%m-%d'):
        t.amount *= 10
    return t


def load_data_from_dir(dirname):
    trades = []
    divs = []
    withholdings = []
    files_names = [x for x in os.listdir(dirname) if x.lower().endswith('.csv')]
    for fn in files_names:
        with open(os.path.join(dirname, fn)) as f:
            report = read_report(f)
            for rec in report['Trades']:
                t = trade_from_report_rec(rec)
                trades.append(t)
            divs += [MoneyInOut(rec, 'Dividend') for rec in report['Dividends']]
            withholdings += [MoneyInOut(rec, 'Withholding') for rec in report['Withholding Tax']]
    trades.sort(key=lambda x: (x.date, x.symbol))
    divs.sort(key=lambda x: (x.date, x.symbol))
    withholdings.sort(key=lambda x: (x.date, x.symbol))
    return trades, divs, withholdings


def main():
    def to_rub_str(t):
        rate = get_usd_rub_exchange_rate_for_date(t.date)
        return '{:0.2f} RUB ({} * {} * {})'.format(t.amount * t.price * rate, t.amount, t.price, rate)

    trades, divs, withholdings = load_data_from_dir('ib_reports')
    print('All trades:')
    for t in trades:
        print('    {}'.format(t))
    print()
    sales, buyings_left = do_the_thing(trades)
    print('Sales ({}):'.format(len(sales)))
    for t in sales:
        inc_rub = get_usd_rub_exchange_rate_for_date(t.date) * t.amount * t.price
        inc_usd = t.price * t.amount
        print('{}: {} {}x${:0.2f}'.format(t.date.strftime('%Y-%m-%d'), t.symbol, t.amount, t.price))
        print('    Income: {} // {}'.format(m(inc_usd), to_rub_str(t)))
        print('    What was sold:')
        exp_rub = decimal.Decimal(0)
        exp_usd = decimal.Decimal(0)
        for sold in t.sold_buyings:
            print('        * {} // {}'.format(sold, to_rub_str(sold)))
            exp_usd += sold.amount * sold.price
            exp_rub += get_usd_rub_exchange_rate_for_date(sold.date) * sold.amount * sold.price
        print('    Profit: {} // {:0.2f} RUB ({:0.2f} - {:0.2f})'.format(m(inc_usd-exp_usd), inc_rub-exp_rub, inc_rub, exp_rub))
        print()
    print()
    print('Buyings left:')
    if len(buyings_left) <= 0:
        print('(none)')
    for t in buyings_left:
        print('    {}'.format(t))
    print()
    print('Dividends ({}):'.format(len(divs)))
    for div in calc_divs(divs, withholdings):
        withholdings_total = abs(sum([w.amount for w in div.withholdings]))
        print('    Dividends from {} on {}, sum: {}, withheld: {}'.format(div.symbol, div.date.strftime('%d.%m.%Y'), m(div.amount), m(withholdings_total)))
        print('        Withholdings:')
        total = decimal.Decimal(0)
        for w in div.withholdings:
            print('            {}'.format(w))
            total += w.amount
        print('            Total: {}'.format(m(total)))
        print()
        


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
        self.assertEqual(len(sales), 2)
        self.assertEqual(len(b_left), 1)


    def testRatesDb(self):
        rate = get_usd_rub_exchange_rate_for_date(datetime.datetime(2018, 7, 27, 9, 33, 38))
        self.assertEqual(rate, decimal.Decimal('62.9471'))
        rate = get_usd_rub_exchange_rate_for_date(datetime.datetime(2020, 1, 5, 9, 33, 38))
        self.assertEqual(rate, decimal.Decimal('61.9057'))

    def testIbReportReadTables(self):
        with open('test_data/test.csv') as f:
            tables = read_report(f)
            self.assertEqual(len(tables), 5)
            self.assertEqual(tables['Trades'][0]['T. Price'], '143.25')

    def testLoadFromDir(self):
        trades, _, _ = load_data_from_dir('test_data')
        self.assertEqual(3, len(trades))




if __name__ == '__main__':
    main()

