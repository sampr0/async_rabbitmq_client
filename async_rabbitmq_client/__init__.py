import logging
import sys

fmtr = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
hdlr = logging.StreamHandler(sys.stdout)
hdlr.setFormatter(fmtr)

rootlogger = logging.getLogger('rabbitmq')
rootlogger.addHandler(hdlr)
rootlogger.setLevel(logging.DEBUG)

from .consumer import RabbitConsumer
from .publisher import RabbitPublisher

from .common import start_thread