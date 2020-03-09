#!/usr/bin/env python3


import unittest
import decimal
import datetime
import copy



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



if __name__ == '__main__':
    main()

