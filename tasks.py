import os
import json
import logging
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from multiprocessing import Pool
from queue import Queue

import xlsxwriter

from external.client import YandexWeatherAPI, YandexWeatherAPIError
from utils import get_url_by_city_name, CITIES, CITIES_TRANSLATION

logger = logging.getLogger(__name__)


class DataFetchingTask:
    """
    Class to fetch weather data via fake YandexWeatherAPI
    """

    def __init__(self, city_names: list, data_dir: str = './data') -> None:
        """
        param city_names: list of city names
        param data_path: path with data files
        """
        super().__init__()
        self._api = YandexWeatherAPI()
        self._city_names = city_names
        self._queue = Queue()
        self._data_dir = data_dir
        try:
            if not os.path.exists(self._data_dir):
                logging.info(f'Cant find directory {self._data_dir}')
                os.makedirs(self._data_dir)
                logging.info(f'Created directory {self._data_dir}')
        except OSError as err:
            logging.error(f'Cant create directory {self._data_dir}: \n{err}')

    def _get_city_data(self, city_name: str) -> dict:
        """
        param city_name: name of city to get data
        """
        logger.debug(f'Fetching data for {city_name}')
        response = {}
        try:
            url_with_data = get_url_by_city_name(city_name)
            response = YandexWeatherAPI.get_forecasting(url_with_data)
            response['city_name'] = city_name
            if 'info' in response:
                response['status'] = 'OK'
                file_path = f'{self._data_dir}/{city_name}_fetched.json'
                with open(file_path, 'w') as json_file:
                    json.dump(response, json_file)
                logger.debug(f'{city_name} data has been fetched and saved into {file_path}')
            else:
                response['status'] = 'No info'
                logger.error(f'Failed {city_name}: \n{response["status"]}')

        except YandexWeatherAPIError as err:
            logger.error(f'Failed {city_name}: \n{err}')
            response['status'] = str(err)
            response['city_name'] = city_name
        return response

    def run(self) -> Queue:
        logger.info('Data fetching started.')
        futures_cities = []
        with ThreadPoolExecutor() as pool:
            futures_cities.extend(pool.submit(self._get_city_data, city_name) for city_name in self._city_names)
            for future in as_completed(futures_cities, timeout=3):
                self._queue.put(future.result())
        logger.info('Data fetching is complete.')
        return self._queue


class DataCalculationTask:
    """
    Class to use analyzer.py to calc statistic indicators
    """

    def __init__(self, city_names: list, data_dir: str = './data') -> None:
        """
        param city_names: list of city names
        param data_path: path with data files
        """
        super().__init__()
        self._city_names = city_names
        self._data_dir = data_dir

    def _calc_city_data(self, city_name: str) -> dict[str, str]:
        """
        param city_name: name of city to calc data
        """
        logger.debug(f'Calculation data for {city_name}')
        input_filename = f'{self._data_dir}/{city_name}_fetched.json'
        output_filename = f'{self._data_dir}/{city_name}_calc.json'
        command_to_execute = [
            'python3',
            './external/analyzer.py',
            '-i',
            input_filename,
            '-o',
            output_filename,
        ]
        calc_result = {'status': 'OK', 'city_name': city_name}
        try:
            run = subprocess.run(command_to_execute, capture_output=True, text=True)
            if run.stderr:
                if 'No such file or directory' in run.stderr:
                    calc_result['status'] = 'No such file or directory'
                else:
                    calc_result['status'] = run.stderr
                logger.error(f'Failed {city_name}: {calc_result["status"]}')
            else:
                logger.debug(f'{city_name} data has been calculated and saved into {output_filename}')

        except subprocess.CalledProcessError as err:
            logger.error(f'Failed {city_name}: \n{err}')
            calc_result['status'] = str(err)
        return calc_result

    def run(self) -> dict:
        logger.info('Data calculation started.')
        with Pool() as pool:
            results = {calc_result['city_name']: calc_result['status'] for calc_result in
                       pool.map(self._calc_city_data, self._city_names)}
        logger.info('Data calculation is complete.')
        return results


class DataAggregationTask:
    """
    Class to read calculated files
    and aggregate weighted average agg_temp_avg and agg_relevant_cond_hours values
    """

    def __init__(self, city_names: list, data_dir: str = './data') -> None:
        """
        param city_names: list of city names
        param data_path: path with data files
        """
        super().__init__()
        self._city_names = city_names
        self._data_dir = data_dir

    @staticmethod
    def _calc_weighted_avg(agg_city_data: dict) -> dict:
        d = agg_city_data['data']
        d['agg_temp_avg'] = 0
        d['agg_relevant_cond_hours'] = 0
        total_hours_count = 0

        for day in d['days']:
            hours_count = day['hours_count']
            if hours_count > 0:
                d['agg_temp_avg'] += hours_count * day['temp_avg']
                d['agg_relevant_cond_hours'] += hours_count * day['relevant_cond_hours']
                total_hours_count += hours_count

        d['agg_temp_avg'] /= total_hours_count
        d['agg_relevant_cond_hours'] /= total_hours_count
        return d

    def _get_agg_city_data(self, city_name: str) -> dict:
        """
        param city_name: name of city to aggregate data
        """
        logger.debug(f'Aggregating data for {city_name}')
        calc_filename = f'{self._data_dir}/{city_name}_calc.json'
        agg_city_data = {'data': {}}
        try:
            with open(calc_filename) as json_file:
                data = json.load(json_file)
                agg_city_data['data'] = data
                agg_city_data['status'] = 'OK'
        except (FileNotFoundError, json.JSONDecodeError) as err:
            logger.error(f'Failed {city_name}: \n{err}')
            agg_city_data['status'] = str(err)
        agg_city_data['city_name'] = city_name
        if agg_city_data.get('data'):
            agg_city_data['data'] = self._calc_weighted_avg(agg_city_data)
        return agg_city_data

    def run(self) -> dict:
        logger.info('Data aggregation started.')
        with Pool() as pool:
            results = {_['city_name']: _ for _ in
                       pool.map(self._get_agg_city_data, self._city_names)}
        logger.info('Data aggregation is complete.')
        return results


class DataAnalyzingTask:
    """
    Class to rank cities and provide output report
    """

    def __init__(self, agg_cities_data: dict, report_filename: str = 'report.xlsx') -> None:
        """
        :param agg_cities_data: aggregated cities data
        :param report_filename: xlsx report filename
        """
        super().__init__()
        self.agg_cities_data = agg_cities_data
        self.report_filename = report_filename

    @staticmethod
    def _rank_cities(agg_cities_data: dict) -> dict:
        logger.info('Ranking cities')
        for city_name in agg_cities_data.copy():
            if agg_cities_data[city_name].get('status', 'error') != 'OK':
                del agg_cities_data[city_name]
                logger.debug(f'{city_name} was skipped, have no info')

        for data in agg_cities_data.values():
            data['data']['days'] = [day for day in data['data'].get('days', []) if day['hours_count'] > 0]

        ranking_cities = [{'city_name': city_name,
                           'agg_temp_avg': data['data']['agg_temp_avg'],
                           'agg_relevant_cond_hours': data['data']['agg_relevant_cond_hours']
                           } for city_name, data in agg_cities_data.items()]

        ranking_cities.sort(key=lambda x: (x['agg_temp_avg'], x['agg_relevant_cond_hours']), reverse=True)

        for i, city_name in enumerate(ranking_cities):
            city_name['rank'] = i + 1

        for city in ranking_cities:
            agg_cities_data[city['city_name']]['rank'] = city['rank']
        return agg_cities_data

    def _find_best_city(self, agg_cities_data: dict) -> list:
        sorted_cities_data = sorted(agg_cities_data.items(), key=lambda x: x[1].get('rank', len(agg_cities_data)))
        sorted_cities = [city for city, data in sorted_cities_data]
        if not sorted_cities:
            logging.info('Got empty list, nothing to analyze')
            return []
        best_cities = [sorted_cities[0]]
        best_city = agg_cities_data[sorted_cities[0]]['data']
        logger.info('The best city to live is:')
        logger.info(CITIES_TRANSLATION.get(best_cities[0], best_cities[0]))
        if len(sorted_cities) > 1:
            for i in range(len(sorted_cities) - 1):
                next_city = agg_cities_data[sorted_cities[i + 1]]['data']
                if next_city['agg_relevant_cond_hours'] == best_city['agg_relevant_cond_hours'] \
                        and next_city['agg_temp_avg'] == best_city['agg_temp_avg']:
                    logger.info(CITIES_TRANSLATION.get(sorted_cities[i + 1], sorted_cities[i + 1]))
                    best_cities.append(sorted_cities[i + 1])
                else:
                    break
        logger.info(f'Average temperature: {best_city["agg_temp_avg"]}')
        logger.info(f'Average condition hours: {best_city["agg_temp_avg"]}')
        try:
            self._generate_output_report(agg_cities_data, sorted_cities)
        except xlsxwriter.exceptions.XlsxWriterException as err:
            logger.error(f'Cant generate output report: \n{err}')
        return best_cities

    def _generate_output_report(self, agg_cities_data: dict, sorted_cities: list) -> None:
        logger.info('Generating output report')
        workbook = xlsxwriter.Workbook(self.report_filename)
        sheet = workbook.add_worksheet()
        sheet.write(0, 0, 'Город / день')
        dates = [day['date'] for day in next(iter(agg_cities_data.values()))['data']['days']]
        for i, date in enumerate(dates):
            sheet.write(0, i + 2, date[-5:])
        sheet.write(0, len(dates) + 2, 'Среднее')
        sheet.write(0, len(dates) + 3, 'Рейтинг')
        curr_row = 0
        format_float = workbook.add_format({'num_format': '0.0'})
        for city in sorted_cities:
            city_data = agg_cities_data[city]
            curr_row += 1
            sheet.write(curr_row, 0, CITIES_TRANSLATION.get(city, city))
            sheet.write(curr_row, 1, 'Температура, среднее')
            for i, day in enumerate(city_data['data']['days']):
                sheet.write(curr_row, i + 2, day['temp_avg'], format_float)
            sheet.write(curr_row, len(dates) + 2, city_data['data']['agg_temp_avg'], format_float)
            sheet.write(curr_row, len(dates) + 3, city_data['rank'])
            curr_row += 1
            sheet.write(curr_row, 1, 'Без осадков, часов')
            for i, day in enumerate(city_data['data']['days']):
                sheet.write(curr_row, i + 2, day['relevant_cond_hours'], format_float)
            sheet.write(curr_row, len(dates) + 2, city_data['data']['agg_relevant_cond_hours'], format_float)
        workbook.close()
        logger.info(f'Report was generated and saved to {self.report_filename}')

    def run(self) -> list:
        logger.info('Data analyzing started.')
        ranked_cities = self._rank_cities(self.agg_cities_data)
        results = self._find_best_city(ranked_cities)
        logger.info('Data analyzing is complete.')
        return results


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    cities = CITIES
    DataFetchingTask(cities).run()
    DataCalculationTask(cities).run()
    agg_data = DataAggregationTask(cities).run()
    DataAnalyzingTask(agg_data).run()
