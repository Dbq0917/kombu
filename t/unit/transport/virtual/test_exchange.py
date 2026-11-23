from __future__ import annotations

from unittest.mock import Mock

import pytest

from kombu import Connection
from kombu.transport.virtual import exchange
from t.mocks import Transport


class ExchangeCase:
    type = None

    def setup_method(self):
        if self.type:
            self.e = self.type(Connection(transport=Transport).channel())


class test_Direct(ExchangeCase):
    type = exchange.DirectExchange
    table = [('rFoo', None, 'qFoo'),
             ('rFoo', None, 'qFox'),
             ('rBar', None, 'qBar'),
             ('rBaz', None, 'qBaz')]

    @pytest.mark.parametrize('exchange,routing_key,default,expected', [
        ('eFoo', 'rFoo', None, {'qFoo', 'qFox'}),
        ('eMoz', 'rMoz', 'DEFAULT', set()),
        ('eBar', 'rBar', None, {'qBar'}),
    ])
    def test_lookup(self, exchange, routing_key, default, expected):
        assert self.e.lookup(
            self.table, exchange, routing_key, default) == expected


class test_Fanout(ExchangeCase):
    type = exchange.FanoutExchange
    table = [(None, None, 'qFoo'),
             (None, None, 'qFox'),
             (None, None, 'qBar')]

    def test_lookup(self):
        assert self.e.lookup(self.table, 'eFoo', 'rFoo', None) == {
            'qFoo', 'qFox', 'qBar',
        }

    def test_deliver_when_fanout_supported(self):
        self.e.channel = Mock()
        self.e.channel.supports_fanout = True
        message = Mock()

        self.e.deliver(message, 'exchange', 'rkey')
        self.e.channel._put_fanout.assert_called_with(
            'exchange', message, 'rkey',
        )

    def test_deliver_when_fanout_unsupported(self):
        self.e.channel = Mock()
        self.e.channel.supports_fanout = False

        self.e.deliver(Mock(), 'exchange', None)
        self.e.channel._put_fanout.assert_not_called()


class test_ConsistentHash(ExchangeCase):
    type = exchange.ConsistentHashExchange
    table = [('rFoo', None, 'qFoo'),
             ('rFoo', None, 'qFox'),
             ('rBar', None, 'qBar'),
             ('rBaz', None, 'qBaz')]

    def test_lookup_returns_single_queue(self):
        result = self.e.lookup(self.table, 'eFoo', 'order-123', None)
        assert len(result) == 1
        assert next(iter(result)) in {'qFoo', 'qFox', 'qBar', 'qBaz'}

    def test_same_routing_key_always_same_queue(self):
        first = self.e.lookup(self.table, 'eFoo', 'order-123', None)
        for _ in range(100):
            assert self.e.lookup(
                self.table, 'eFoo', 'order-123', None) == first

    def test_routing_is_stable_regardless_of_binding_order(self):
        reversed_table = list(reversed(self.table))
        for key in ('order-1', 'order-2', 'order-3'):
            assert self.e.lookup(self.table, 'eFoo', key, None) == \
                self.e.lookup(reversed_table, 'eFoo', key, None)

    def test_distributes_keys_across_all_queues(self):
        selected = {
            next(iter(self.e.lookup(
                self.table, 'eFoo', f'order-{i}', None)))
            for i in range(200)
        }
        assert selected == {'qFoo', 'qFox', 'qBar', 'qBaz'}

    def test_lookup_without_bindings(self):
        assert self.e.lookup([], 'eFoo', 'order-1', 'DEFAULT') == {'DEFAULT'}
        assert self.e.lookup([], 'eFoo', 'order-1', None) == set()

    def test_duplicate_bindings_counted_once(self):
        duplicated = self.table + self.table
        result = self.e.lookup(duplicated, 'eFoo', 'order-1', None)
        assert len(result) == 1

    def test_deliver(self):
        self.e.channel = Mock()
        self.e.channel._lookup.return_value = ['qFoo']
        message = Mock()
        self.e.deliver(message, 'exchange', 'order-1')
        self.e.channel._put.assert_called_once_with(
            'qFoo', message,
        )


class test_Topic(ExchangeCase):
    type = exchange.TopicExchange
    table = [
        ('stock.#', None, 'rFoo'),
        ('stock.us.*', None, 'rBar'),
    ]

    def setup_method(self):
        super().setup_method()
        self.table = [(rkey, self.e.key_to_pattern(rkey), queue)
                      for rkey, _, queue in self.table]

    def test_prepare_bind(self):
        x = self.e.prepare_bind('qFoo', 'eFoo', 'stock.#', {})
        assert x == ('stock.#', r'^stock\..*?$', 'qFoo')

    @pytest.mark.parametrize('exchange,routing_key,default,expected', [
        ('eFoo', 'stock.us.nasdaq', None, {'rFoo', 'rBar'}),
        ('eFoo', 'stock.europe.OSE', None, {'rFoo'}),
        ('eFoo', 'stockxeuropexOSE', None, set()),
        ('eFoo', 'candy.schleckpulver.snap_crackle', None, set()),
        # '*' is a single word: only 'stock.#' may match a longer key.
        ('eFoo', 'stock.us.nasdaq.tech', None, {'rFoo'}),
    ])
    def test_lookup(self, exchange, routing_key, default, expected):
        assert self.e.lookup(
            self.table, exchange, routing_key, default) == expected
        assert self.e._compiled

    def test_deliver(self):
        self.e.channel = Mock()
        self.e.channel._lookup.return_value = ('a', 'b')
        message = Mock()
        self.e.deliver(message, 'exchange', 'rkey')

        assert self.e.channel._put.call_args_list == [
            (('a', message), {}),
            (('b', message), {}),
        ]


class test_TopicMultibind(ExchangeCase):
    # Testing message delivery in case of multiple overlapping
    # bindings for the same queue. As AMQP states, in case of
    # overlapping bindings, a message must be delivered once to
    # each matching queue.
    type = exchange.TopicExchange
    table = [
        ('stock', None, 'rFoo'),
        ('stock.#', None, 'rFoo'),
        ('stock.us.*', None, 'rFoo'),
        ('#', None, 'rFoo'),
    ]

    def setup_method(self):
        super().setup_method()
        self.table = [(rkey, self.e.key_to_pattern(rkey), queue)
                      for rkey, _, queue in self.table]

    @pytest.mark.parametrize('exchange,routing_key,default,expected', [
        ('eFoo', 'stock.us.nasdaq', None, {'rFoo'}),
        ('eFoo', 'stock.europe.OSE', None, {'rFoo'}),
        ('eFoo', 'stockxeuropexOSE', None, {'rFoo'}),
        ('eFoo', 'candy.schleckpulver.snap_crackle', None, {'rFoo'}),
    ])
    def test_lookup(self, exchange, routing_key, default, expected):
        assert self.e._compiled
        assert self.e.lookup(
            self.table, exchange, routing_key, default) == expected


class test_ExchangeType(ExchangeCase):
    type = exchange.ExchangeType

    def test_lookup(self):
        with pytest.raises(NotImplementedError):
            self.e.lookup([], 'eFoo', 'rFoo', None)

    def test_prepare_bind(self):
        assert self.e.prepare_bind('qFoo', 'eFoo', 'rFoo', {}) == (
            'rFoo', None, 'qFoo',
        )

    e1 = {
        'type': 'direct',
        'durable': True,
        'auto_delete': True,
        'arguments': {},
    }
    e2 = dict(e1, arguments={'expires': 3000})

    @pytest.mark.parametrize('ex,eq,name,type,durable,auto_delete,arguments', [
        (e1, True, 'eFoo', 'direct', True, True, {}),
        (e1, False, 'eFoo', 'topic', True, True, {}),
        (e1, False, 'eFoo', 'direct', False, True, {}),
        (e1, False, 'eFoo', 'direct', True, False, {}),
        (e1, False, 'eFoo', 'direct', True, True, {'expires': 3000}),
        (e2, True, 'eFoo', 'direct', True, True, {'expires': 3000}),
        (e2, False, 'eFoo', 'direct', True, True, {'expires': 6000}),
    ])
    def test_equivalent(
            self, ex, eq, name, type, durable, auto_delete, arguments):
        is_eq = self.e.equivalent(
            ex, name, type, durable, auto_delete, arguments)
        assert is_eq if eq else not is_eq
