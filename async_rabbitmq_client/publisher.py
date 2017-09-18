#!/usr/bin/env python
#coding:utf-8
"""
  Author:   --<v1ll4n>
  Purpose: Publisher
  Created: 09/18/17
"""

import pika

try:
    import queue
except:
    import Queue as queue

import time
import logging

from .common import BaseClient

class RabbitPublisher(BaseClient):
    """"""

    def initial(self):
        """"""
        self.logger = logging.getLogger('rabbitmq.publisher')
        self._working = True
        self.default_exchange = None
        self.default_publish_interval = 0.1
        self.default_routing_key = 0.1
        self.publish_queue = queue.Queue()
    
    @property
    def exchanges(self):
        """"""
        return [i['exchange'] for i in self._exchange_list]
    
    def set_default_exchange(self, exchange_name):
        """"""
        assert exchange_name in self.exchanges, 'you should add this exchange first.'
        
        self.default_exchange = exchange_name
    
    def set_default_routing_key(self, routing_key):
        """"""
        self.default_routing_key = routing_key
        
    def set_publish_interval(self, publish_interval=0.1):
        """"""
        self.default_publish_interval = publish_interval
        
    def confirm_delivery(self):
        """"""
        self.channel.confirm_delivery(self.on_confirm_delivery)
    
    def on_confirm_delivery(self, method_frame):
        """"""
        self.logger.debug('recv a confirm information.')
    
    def publish_message(self):
        """"""
        # from publish_queue getting message
        body, exchange, routingkey = self.publish_queue.get()
        
        # default
        exchange = exchange if exchange else self.default_exchange
        routingkey = routingkey if routingkey else self.default_routing_key
        
        self.logger.debug('publish a message:{} from publish queue to {}:{}'.\
                          format(body, exchange, routingkey))
        
        try: 
            self.channel.basic_publish(
                exchange,
                routingkey,
                body,
                )
            
        except:
            self.logger.debug('publish failed.')
            self.publish_queue.put((body, exchange, routingkey))
        
        return time.time() + self.default_publish_interval
            
    def main(self):
        """"""
        self.logger.debug('enter the main loop')
        next_pub = 0
        while self._working:
            if not self.publish_queue.empty():
                now = time.time()
                if now >= next_pub:
                    next_pub = self.publish_message()
        
        self.logger.debug('exit the main loop')