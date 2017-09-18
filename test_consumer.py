#!/usr/bin/env python
#coding:utf-8
"""
  Author:   --<>
  Purpose: 
  Created: 09/18/17
"""

import unittest

from async_rabbitmq_client import RabbitConsumer, start_thread

class ConsumerTester(unittest.TestCase):
    """"""

    def test_consumer(self):
        """"""
        config = {}
        consumer = RabbitConsumer(
            host='127.0.0.1',
            port=5672,
            vhost='rabbitvhost',
            username='guest',
            password='guest',
            queue_name='testqueue',
            **config
        )
        
        consumer.add_exchange('testexchange', 'direct')
        consumer.add_queue_bind('testexchange', 'testkey')
        
        aqueue = consumer.get_message_queue()
        
        ret = start_thread(consumer.run)
        
        while True:
            if aqueue.empty():
                pass
            else:
                print(aqueue.get())

if __name__ == '__main__':
    unittest.main()