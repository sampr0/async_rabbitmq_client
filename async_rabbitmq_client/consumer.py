#!/usr/bin/env python
#coding:utf-8
"""
  Author:   --<v1ll4n>
  Purpose: Consumer
  Created: 09/18/17
"""
try:
    import queue
except:
    import Queue as queue

import logging

from .common import BaseClient

class RabbitConsumer(BaseClient):
    """"""

    def initial(self):
        """"""
        self.logger = logging.getLogger('rabbitmq.consumer')
        self.message_queue = queue.Queue()

    def main(self):
        """"""
        self.consumer_tag = self.channel.basic_consume(
            consumer_callback=self.on_message_in,
            queue=self.rabbit_queue_name,
        )
    
    def on_message_in(self, ch, method, properties, body):
        """"""
        self.logger.debug('recv a message: {}'.format(body))
        
        # process result
        self.message_queue.put((body, method, properties))
        
        self.ack_message(method.delivery_tag)
    
    def ack_message(self, tag):
        """ACK the message to rabbitmq."""
        # ACK the message dtag:{}
        self.logger.debug('ack the message-dtag:{}'.format(tag))
        self.channel.basic_ack(tag)
    
    def main_stop(self):
        """"""
        self.logger.debug('stopping consuming messages.')
        self.channel.basic_cancel(self.on_cancelok)
    
    def on_cancelok(self, *vargs, **kwargs):
        """"""
        self.logger.debug('cancel consuming success.')
        
    def get_message_queue(self):
        """"""
        return self.message_queue
    
    