"""Contains functionality related to Weather"""
import logging

logger = logging.getLogger(__name__)


class Weather:
    """Defines the Weather model"""

    def __init__(self):
        """Creates the weather model"""
        self.temperature = 70.0
        self.status = "sunny"

    def process_message(self, message):
        """Handles incoming weather data"""
        data = message.value()
        self.temperature = data['temperature']
        self.status = data['status']

        logger.info(f"processed weather -> {self.temperature} : {self.status}")
