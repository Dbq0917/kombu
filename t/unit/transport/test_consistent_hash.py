from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import uuid

from kombu import Connection, Consumer, Exchange, Producer, Queue

QUEUE_COUNT = 4
MESSAGE_COUNT = 100
PARTITION_KEY = 'order-42'


class test_ConsistentHashExchangeMemory:
    def setup_method(self):
        self.connection = Connection(transport='memory')
        self.channel = self.connection.channel()
        suffix = uuid.uuid4().hex
        self.exchange = Exchange(
            f'test_consistent_hash_memory_{suffix}',
            type='consistent-hash')
        self.queues = [
            Queue(f'test_consistent_hash_memory_{suffix}.q{index}',
                  exchange=self.exchange, routing_key='partition')
            for index in range(QUEUE_COUNT)
        ]
        for queue in self.queues:
            queue.declare(channel=self.channel)

    def test_same_key_always_routes_to_one_queue(self):
        producer = Producer(self.channel, self.exchange)

        for index in range(MESSAGE_COUNT):
            producer.publish({'seq': index}, routing_key=PARTITION_KEY)

        occupied = [
            queue.name for queue in self.queues
            if self.channel._size(queue.name)
        ]
        assert occupied == [
            queue.name for queue in self.queues
            if self.channel._size(queue.name) == MESSAGE_COUNT
        ]
        assert len(occupied) == 1

    def test_messages_consumed_in_publish_order(self):
        producer = Producer(self.channel, self.exchange)
        for index in range(MESSAGE_COUNT):
            producer.publish({'seq': index}, routing_key=PARTITION_KEY)

        target = next(
            queue for queue in self.queues
            if self.channel._size(queue.name) == MESSAGE_COUNT
        )
        received = []
        consumer = Consumer(self.channel, [target], no_ack=True)

        def callback(body, message):
            received.append(body['seq'])

        consumer.register_callback(callback)
        consumer.consume()
        while len(received) < MESSAGE_COUNT:
            self.connection.drain_events()

        assert received == list(range(MESSAGE_COUNT))

    def test_different_keys_can_use_different_queues(self):
        producer = Producer(self.channel, self.exchange)
        for index in range(MESSAGE_COUNT):
            producer.publish(
                {'seq': index}, routing_key=f'order-{index}')

        occupied = {
            queue.name for queue in self.queues
            if self.channel._size(queue.name)
        }
        assert len(occupied) > 1


CONSUMER_SCRIPT = r'''
import json
import sys

from kombu import Connection, Consumer, Queue

data_folder, control_folder, queue_name, result_path = sys.argv[1:5]
connection = Connection(
    transport='filesystem',
    transport_options={
        'data_folder_in': data_folder,
        'data_folder_out': data_folder,
        'control_folder': control_folder,
        'polling_interval': 0.05,
    },
)
channel = connection.channel()
received = []
consumer = Consumer(channel, [Queue(queue_name)], no_ack=True)

def callback(body, message):
    received.append(body['seq'])

consumer.register_callback(callback)
consumer.consume()
while len(received) < %d:
    try:
        connection.drain_events(timeout=5)
    except Exception:
        break

with open(result_path, 'w') as handle:
    json.dump({'queue': queue_name, 'received': received}, handle)
''' % MESSAGE_COUNT


class test_ConsistentHashExchangeFourProcesses:
    def setup_method(self):
        self.data_folder = tempfile.mkdtemp()
        self.control_folder = tempfile.mkdtemp()
        self.result_folder = tempfile.mkdtemp()

    def teardown_method(self):
        for folder in (
                self.data_folder, self.control_folder, self.result_folder):
            shutil.rmtree(folder, ignore_errors=True)

    def _connection(self):
        return Connection(
            transport='filesystem',
            transport_options={
                'data_folder_in': self.data_folder,
                'data_folder_out': self.data_folder,
                'control_folder': self.control_folder,
                'polling_interval': 0.05,
            },
        )

    def test_hundred_same_key_messages_stay_ordered(self):
        exchange = Exchange(
            'test_consistent_hash_processes', type='consistent-hash')
        queues = [
            Queue(f'test_consistent_hash_processes.q{index}',
                  exchange=exchange, routing_key='partition')
            for index in range(QUEUE_COUNT)
        ]

        producer_connection = self._connection()
        producer_channel = producer_connection.channel()
        for queue in queues:
            queue.declare(channel=producer_channel)
        producer = Producer(producer_channel, exchange)
        for index in range(MESSAGE_COUNT):
            producer.publish({'seq': index}, routing_key=PARTITION_KEY)
        producer_connection.close()

        processes = []
        result_paths = []
        for queue in queues:
            result_path = os.path.join(
                self.result_folder, f'{queue.name}.json')
            result_paths.append(result_path)
            processes.append(subprocess.Popen(
                [sys.executable, '-c', CONSUMER_SCRIPT,
                 self.data_folder, self.control_folder,
                 queue.name, result_path],
            ))

        for process in processes:
            assert process.wait(timeout=60) == 0

        results = []
        for result_path in result_paths:
            with open(result_path) as handle:
                results.append(json.load(handle))

        active = [result for result in results if result['received']]
        assert len(active) == 1
        assert active[0]['received'] == list(range(MESSAGE_COUNT))
