#!/usr/bin/env python
#coding:utf-8
"""
  Author:  v1ll4n --<>
  Purpose: BaseClient
  Created: 09/18/17
"""

from threading import Thread
try:
    import queue
except:
    import Queue as queue

import logging

import pika


class BaseClient(object):
    """"""

    #----------------------------------------------------------------------
    def __init__(self, host='127.0.0.1', 
                 port=5672, vhost='/',
                 username='guest', 
                 password='guest',
                 queue_name='test',
                 queue_delare_config={},
                 **config):
        """Constructor"""
        # user defined
        self._host = host
        self._port = port
        self._vhost = vhost
        self._username = username
        self._password = password
        self.rabbit_queue_name = queue_name
        assert isinstance(queue_delare_config, dict), 'queue config must be a dict'
        self.rabbit_queue_config = queue_delare_config

        # extra connection config
        self._config = config

        self._closed = False

        # setting
        self._cred = pika.PlainCredentials(self._username,
                                           self._password)
        self._param = pika.ConnectionParameters(
            host=self._host,
            port=self._port,
            credentials=self._cred,
            virtual_host=self._vhost,
            **self._config
        )
        
        # logger
        self.logger = logging.getLogger('rabbitmq')

        self._exchange_list = []
        self._exchange_map_rkey = {}

        # buffer
        self._exchange_buffer_queue = queue.Queue()
        self._bind_buffer_queue = queue.Queue()
        
        
        self.initial()
    
    def initial(self):
        """
        User defined init
        """
        

    def add_exchange(self, exchange, exchange_type, **config):
        """"""
        params = {}
        params['exchange'] = exchange
        params['exchange_type'] = exchange_type

        params.update(config)
        self._exchange_list.append(params)


    def add_queue_bind(self, exchange, routing_key):
        """"""
        assert isinstance(routing_key, str) and routing_key != '', 'not a valid rkey.'
        if exchange not in self._exchange_list:
            self._exchange_map_rkey[exchange] = []

        self._exchange_map_rkey[exchange].append(routing_key)

    @property
    def bind_count(self):
        """"""
        tmp = 0
        for _, rlist in self._exchange_map_rkey.items():
            tmp = tmp + len(rlist)

        return tmp


    def connect(self):
        """Connect it"""

        self.logger.debug('Connecting: {}:{}/{} with {}'.format(self._host,
                                                           self._port,
                                                           self._vhost,
                                                           self._username))
        self.connection = pika.SelectConnection(
            parameters=self._param,
            stop_ioloop_on_close=False,
            on_open_callback=self.on_conn_open
        )

        return self.connection

    def on_conn_open(self, conn):
        """"""
        assert isinstance(conn, pika.SelectConnection)
        self.logger.debug('Connected')
        conn.add_on_close_callback(self.on_conn_closed)

        self.logger.debug('Opening channel.')
        self.open_channel()

    def on_conn_closed(self, conn, code=200, text='user abort'):
        """If connection lose unexpected, the consumer will reconnect"""
        self.channel = None
        if self._closed:
            self.connection.ioloop.stop()
        else:
            self.logger.debug('ConnectionClosed, try reconnected.')
            self.connection.add_timeout(1, self.reconnect)

    def reconnect(self):
        """"""

        # close the old connection ioloop
        self.connection.ioloop.stop()

        self.logger.debug('Reconnect to rabbitmq.')
        if not self._closed:
            self.connect()
            self.connection.ioloop.start()

    def open_channel(self):
        """"""
        self.logger.debug('Creating a channel.')

        self.connection.channel(self.on_ch_open)

    def on_ch_open(self, channel):
        """"""
        self.logger.debug('Created.')
        assert isinstance(channel, pika.channel.Channel)
        self.channel = channel
        channel.add_on_close_callback(self.on_ch_closed)

        self.setup()

    def on_ch_closed(self, ch, code=200, reason="user abort"):
        """"""
        self.logger.debug('Channel is closed, because of {}:{}'.format(code, reason))

        # when you do bad to your channel.
        # the channel will be closed.
        # so you should re-create channel
        self.open_channel()

    def setup(self):
        """"""
        self.logger.debug("Setup the exchagnes.")

        if self._exchange_list:
            # exchanges setup
            for i in self._exchange_list:
                self.channel.exchange_declare(
                    callback=self.setup_exchange_and_bind_queue,
                    **i
                )
        else:
            self.setup_exchange_and_bind_queue()



    def setup_exchange_and_bind_queue(self, *args, **kw):
        """"""
        if self._exchange_list:
            self._exchange_buffer_queue.put(1)
    
            if self._exchange_buffer_queue.qsize() == len(self._exchange_list):
                while not self._exchange_buffer_queue.empty():
                    self._exchange_buffer_queue.get()
    
                self.logger.debug('setup exchanges success.')
                self.setup_queue()
        else:
            self.setup_queue()
            

    def setup_queue(self):
        """"""
        self.logger.debug('declare queue:{}'.format(self.rabbit_queue_name))
        self.channel.queue_declare(
            callback=self.on_queue_declare_ok,
            queue=self.rabbit_queue_name,
            **self.rabbit_queue_config
        )

    def on_queue_declare_ok(self, *args, **kwargs):
        """"""
        # no exchange, just ignore them
        if len(self._exchange_list) == 0:
            self.main()
            return

        # bind the exchanges
        self.logger.debug('binding the routingkey and exchanges')
        for exchange, rkeys in self._exchange_map_rkey.items():

            for rk in rkeys:
                self.channel.queue_bind(
                    callback=self.on_bindok,
                    queue=self.rabbit_queue_name,
                    exchange=exchange,
                    routing_key=rk
                )
        
        self.main()


    def on_bindok(self, *args, **kw):
        """"""
        self._bind_buffer_queue.put(1)

        if self._bind_buffer_queue.qsize() == self.bind_count:
            while not self._bind_buffer_queue.empty():
                self._bind_buffer_queue.get()

            self.logger.debug('binding success, now start the main')
            self.main()

    def main(self):
        """"""
        pass

    def main_stop(self):
        """"""
        pass

    def reset_channel(self):
        """"""
        self.logger.debug('reset channel')
        self.channel.close()

    def run(self):
        """"""
        self.logger.debug('start up.')
        self.connection = self.connect()
        self.connection.ioloop.start()

    def stop(self):
        """"""
        self.logger.debug('stopping.')
        self._closed = True
        self.main_stop()

        self.connection.close()

def start_thread(target, name=None):
    """"""
    assert callable(target)
    ret = Thread(name=name, target=target)
    ret.daemon = True
    ret.start()
    return ret