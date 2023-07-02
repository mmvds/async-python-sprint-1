import unittest
import logging
from multiprocessing import Process
from queue import Queue

from tasks import (DataAggregationTask,
                   DataAnalyzingTask,
                   DataCalculationTask,
                   DataFetchingTask)

logging.basicConfig(level=logging.DEBUG)


class DataFetchingTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        logging.debug("Data fetching tests started...")

    @classmethod
    def tearDownClass(cls) -> None:
        logging.debug("Data fetching tests finished...")

    def test_valid_data(self):
        queue = DataFetchingTask(['MOSCOW', 'LONDON', 'BERLIN']).run()
        fetched_data = {}
        for _ in range(queue.qsize()):
            item = queue.get()
            fetched_data[item['city_name']] = item
        self.assertEqual(len(fetched_data), 3)
        self.assertEqual(fetched_data['BERLIN']['status'], 'OK')
        self.assertEqual(fetched_data['LONDON']['info']['lon'], 0.07)
        self.assertIn('forecasts', fetched_data['MOSCOW'])

    def test_extra_data(self):
        queue = DataFetchingTask(['TORONTO']).run()
        self.assertIn('Extra data', queue.get()['status'])

    def test_not_found_data(self):
        queue = DataFetchingTask(['GIZA']).run()
        self.assertIn('HTTP Error 404', queue.get()['status'])


class DataCalculationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        logging.debug("Data calculating tests started...")

    @classmethod
    def tearDownClass(cls) -> None:
        logging.debug("Data calculating tests finished...")

    def test_valid_data(self):
        self.assertEqual(DataCalculationTask(['MOSCOW', 'LONDON', 'BERLIN']).run(),
                         {'MOSCOW': 'OK', 'LONDON': 'OK', 'BERLIN': 'OK'})

    def test_not_found_file(self):
        self.assertIn('No such file or directory', DataCalculationTask(['GIZA']).run()['GIZA'])


class DataAggregationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        logging.debug("Data aggregation tests started...")

    @classmethod
    def tearDownClass(cls) -> None:
        logging.debug("Data aggregation tests finished...")

    def test_valid_data(self):
        self.assertEqual(len(DataAggregationTask(['MOSCOW', 'LONDON', 'BERLIN']).run()), 3)
        self.assertIn('agg_temp_avg', DataAggregationTask(['MOSCOW', 'LONDON', 'BERLIN']).run()['MOSCOW']['data'])
        self.assertIn('agg_relevant_cond_hours', DataAggregationTask(['LONDON']).run()['LONDON']['data'])

    def test_not_found_file(self):
        self.assertIn('No such file or directory', DataAggregationTask(['GIZA']).run()['GIZA']['status'])


class DataDataAnalyzingTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        logging.debug("Data analyzing tests started...")

    @classmethod
    def tearDownClass(cls) -> None:
        logging.debug("Data analyzing tests finished...")

    def test_find_best_city(self):
        agg_data = DataAggregationTask(['MOSCOW', 'LONDON', 'BERLIN', 'GIZA']).run()
        self.assertEqual(DataAnalyzingTask(agg_data).run(), ['BERLIN'])


if __name__ == "__main__":
    unittest.main(module=__name__)
