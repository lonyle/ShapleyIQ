import logging

# we can only use the raw way to import config because recurrent dependencies
import yaml
import os
from os.path import dirname, abspath, join
ROOT_DIR = dirname(dirname(dirname(abspath(__file__))))
LOG_FOLDER = join(ROOT_DIR, 'log/')


def log_level_parse(level: str):
    l = level.upper()
    level_dict={
        'CRITICAL':50,
        'ERROR':40,
        'WARNING':30,
        'INFO':20,
        'DEBUG':10,
        'NOTSET':0,
    }
    return level_dict.get(l, 20)

def setup_logger(name, module_name='anomalydetection'):
    env_level = os.environ.get("RCA_LOG_LEVEL", "INFO")
    

    formatter = logging.Formatter(fmt='%(asctime)s - \
            %(levelname)s - \
            %(module)s - \
            %(funcName)s(): \
            line-%(lineno)d: \n \
            Message:\t%(message)s')

    # formatter = logging.Formatter(
    #     '%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)    
    if env_level == 'DEBUG':
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    if not logger.hasHandlers():
        logger.addHandler(handler)

    return logger
