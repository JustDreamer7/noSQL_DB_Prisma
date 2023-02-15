import logging


class Logger:

    def __init__(self):
        self.logger = logging.getLogger("exampleApp")
        self.logger.setLevel(logging.INFO)
        fh = logging.FileHandler('logs/info.log')
        fh.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        self.logger.addHandler(fh)
        self.logger.info("Program started")
