

"""Test for the BigQuery tornadoes example."""

import logging
import unittest

import apache_beam as beam
from apache_beam.examples.cookbook import bigquery_tornadoes
from apache_beam.test_pipeline import TestPipeline


class BigQueryTornadoesTest(unittest.TestCase):

  def test_basics(self):
    p = TestPipeline()
    rows = (p | 'create' >> beam.Create([
        {'month': 1, 'day': 1, 'tornado': False},
        {'month': 1, 'day': 2, 'tornado': True},
        {'month': 1, 'day': 3, 'tornado': True},
        {'month': 2, 'day': 1, 'tornado': True}]))
    results = bigquery_tornadoes.count_tornadoes(rows)
    beam.assert_that(results, beam.equal_to([{'month': 1, 'tornado_count': 2},
                                             {'month': 2, 'tornado_count': 1}]))
    p.run().wait_until_finish()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
