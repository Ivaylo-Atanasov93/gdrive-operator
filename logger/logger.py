import logging


logger = logging.getLogger('G-Drive')
logger.setLevel(level=logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s: %(message)s'
)
ch.setFormatter(formatter)
logger.addHandler(ch)