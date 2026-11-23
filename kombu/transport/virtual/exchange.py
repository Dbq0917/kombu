"""Virtual AMQ Exchange.

Implementations of the standard exchanges defined
by the AMQ protocol  (excluding the `headers` exchange).
"""

from __future__ import annotations

import hashlib
import re

from kombu.utils.text import escape_regex


class ExchangeType:
    """Base class for exchanges.

    Implements the specifics for an exchange type.

    Arguments:
    ---------
        channel (ChannelT): AMQ Channel.
    """

    type = None

    def __init__(self, channel):
        self.channel = channel

    def lookup(self, table, exchange, routing_key, default):
        """Lookup all queues matching `routing_key` in `exchange`.

        Returns
        -------
            str: queue name, or 'default' if no queues matched.
        """
        raise NotImplementedError('subclass responsibility')

    def prepare_bind(self, queue, exchange, routing_key, arguments):
        """Prepare queue-binding.

        Returns
        -------
            Tuple[str, Pattern, str]: of `(routing_key, regex, queue)`
                to be stored for bindings to this exchange.
        """
        return routing_key, None, queue

    def equivalent(self, prev, exchange, type,
                   durable, auto_delete, arguments):
        """Return true if `prev` and `exchange` is equivalent."""
        return (type == prev['type'] and
                durable == prev['durable'] and
                auto_delete == prev['auto_delete'] and
                (arguments or {}) == (prev['arguments'] or {}))


class DirectExchange(ExchangeType):
    """Direct exchange.

    The `direct` exchange routes based on exact routing keys.
    """

    type = 'direct'

    def lookup(self, table, exchange, routing_key, default):
        return {
            queue for rkey, _, queue in table
            if rkey == routing_key
        }

    def deliver(self, message, exchange, routing_key, **kwargs):
        _lookup = self.channel._lookup
        _put = self.channel._put
        for queue in _lookup(exchange, routing_key):
            _put(queue, message, **kwargs)


class TopicExchange(ExchangeType):
    """Topic exchange.

    The `topic` exchange routes messages based on words separated by
    dots, using wildcard characters ``*`` (any single word), and ``#``
    (one or more words).
    """

    type = 'topic'

    #: map of wildcard to regex conversions
    wildcards = {'*': r'[^\.]+',
                 '#': r'.*?'}

    #: compiled regex cache
    _compiled = {}

    def lookup(self, table, exchange, routing_key, default):
        return {
            queue for rkey, pattern, queue in table
            if self._match(pattern, routing_key)
        }

    def deliver(self, message, exchange, routing_key, **kwargs):
        _lookup = self.channel._lookup
        _put = self.channel._put
        deadletter = self.channel.deadletter_queue
        for queue in [q for q in _lookup(exchange, routing_key)
                      if q and q != deadletter]:
            _put(queue, message, **kwargs)

    def prepare_bind(self, queue, exchange, routing_key, arguments):
        return routing_key, self.key_to_pattern(routing_key), queue

    def key_to_pattern(self, rkey):
        """Get the corresponding regex for any routing key."""
        return '^%s$' % (r'\.'.join(
            self.wildcards.get(word, word)
            for word in escape_regex(rkey, '.#*').split('.')
        ))

    def _match(self, pattern, string):
        """Match regular expression (cached).

        Same as :func:`re.match`, except the regex is compiled and cached,
        then reused on subsequent matches with the same pattern.
        """
        try:
            compiled = self._compiled[pattern]
        except KeyError:
            compiled = self._compiled[pattern] = re.compile(pattern, re.U)
        return compiled.match(string)


class FanoutExchange(ExchangeType):
    """Fanout exchange.

    The `fanout` exchange implements broadcast messaging by delivering
    copies of all messages to all queues bound to the exchange.

    To support fanout the virtual channel needs to store the table
    as shared state.  This requires that the `Channel.supports_fanout`
    attribute is set to true, and the `Channel._queue_bind` and
    `Channel.get_table` methods are implemented.

    See Also
    --------
        the redis backend for an example implementation of these methods.
    """

    type = 'fanout'

    def lookup(self, table, exchange, routing_key, default):
        return {queue for _, _, queue in table}

    def deliver(self, message, exchange, routing_key, **kwargs):
        if self.channel.supports_fanout:
            self.channel._put_fanout(
                exchange, message, routing_key, **kwargs)


class ConsistentHashExchange(ExchangeType):
    """Consistent-hash (key-partitioned) exchange.

    The ``consistent-hash`` exchange guarantees that every message
    carrying the same routing key is routed to exactly one of the queues
    bound to the exchange.  Because the selected queue never changes for
    a given key, messages belonging to the same entity (for example the
    status updates of a single order) are always consumed from a single
    queue and therefore stay in publish order, while different keys can
    still be consumed in parallel from the other queues.

    The destination queue is chosen by hashing the routing key with
    :func:`hashlib.md5` and taking the result modulo the number of bound
    queues.  ``md5`` is used purely as a stable, well distributed
    partitioning function; the same hash is produced by every process so
    independent worker processes agree on the destination queue.
    """

    type = 'consistent-hash'

    def _queues(self, table):
        # Sort and de-duplicate the bound queues so that processes that
        # observed bindings in a different order still make the same
        # routing decision.
        return sorted({queue for _, _, queue in table if queue})

    def _route(self, routing_key, queues):
        key_hash = int(
            hashlib.md5(routing_key.encode('utf-8')).hexdigest(), 16
        )
        return queues[key_hash % len(queues)]

    def lookup(self, table, exchange, routing_key, default):
        queues = self._queues(table)
        if not queues:
            return {default} if default is not None else set()
        return {self._route(routing_key, queues)}

    def deliver(self, message, exchange, routing_key, **kwargs):
        _lookup = self.channel._lookup
        _put = self.channel._put
        for queue in _lookup(exchange, routing_key):
            if queue:
                _put(queue, message, **kwargs)


#: Map of standard exchange types and corresponding classes.
STANDARD_EXCHANGE_TYPES = {
    'direct': DirectExchange,
    'topic': TopicExchange,
    'fanout': FanoutExchange,
    'consistent-hash': ConsistentHashExchange,
}
