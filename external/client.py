import json
import logging
from http import HTTPStatus
from urllib.request import urlopen

logger = logging.getLogger()


class YandexWeatherAPIError(Exception):
    def __init__(self, message):
        self.message = message
        super().__init__(message)


class YandexWeatherAPI:
    """Base class for requests"""

    @staticmethod
    def __do_req(url: str) -> str:
        """Base request method"""
        try:
            with urlopen(url) as response:
                resp_body = response.read().decode("utf-8")
                data = json.loads(resp_body)

            if response.status != HTTPStatus.OK:
                raise YandexWeatherAPIError(f"Error during execute request. {response.status}: {response.reason}")
            return data

        except Exception as ex:
            logger.error(ex)
            raise YandexWeatherAPIError(f"Unexpected error: {ex}") from ex

    @staticmethod
    def get_forecasting(url: str):
        """
        :param url: url_to_json_data as str
        :return: response data as json
        """
        return YandexWeatherAPI.__do_req(url)
