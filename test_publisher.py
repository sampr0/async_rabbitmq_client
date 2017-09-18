#!/usr/bin/env python
#coding:utf-8
"""
  Author:   --<v1ll4n>
  Purpose: Tests
  Created: 09/18/17
"""


from async_rabbitmq_client import RabbitPublisher

publisher = RabbitPublisher(vhost='rabbitvhost')
publisher.add_exchange('testexchange', 'direct')
publisher.set_default_exchange('testexchange')
publisher.set_default_routing_key('testkey')

for i in range(43):
    publisher.publish_queue.put(("asdfasdfasdf-{}".format(i), None, None))

publisher.run()