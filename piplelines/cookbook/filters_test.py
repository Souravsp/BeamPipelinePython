

"""Test for the filters example."""

import logging
import unittest

import apache_beam as beam
from apache_beam.examples.cookbook import filters
from apache_beam.test_pipeline import TestPipeline


class FiltersTest(unittest.TestCase):
  # Note that 'removed' should be projected away by the pipeline
  input_data = [
      {'year': 2010, 'month': 1, 'day': 1, 'mean_temp': 3, 'removed': 'a'},
      {'year': 2012, 'month': 1, 'day': 2, 'mean_temp': 3, 'removed': 'a'},
      {'year': 2011, 'month': 1, 'day': 3, 'mean_temp': 5, 'removed': 'a'},
      {'year': 2013, 'month': 2, 'day': 1, 'mean_temp': 3, 'removed': 'a'},
      {'year': 2011, 'month': 3, 'day': 3, 'mean_temp': 5, 'removed': 'a'},
      ]

  def _get_result_for_month(self, month):
    p = TestPipeline()
    rows = (p | 'create' >> beam.Create(self.input_data))

    results = filters.filter_cold_days(rows, month)
    return results

  def test_basic(self):
    """Test that the correct result is returned for a simple dataset."""
    results = self._get_result_for_month(1)
    beam.assert_that(
        results,
        beam.equal_to([{'year': 2010, 'month': 1, 'day': 1, 'mean_temp': 3},
                       {'year': 2012, 'month': 1, 'day': 2, 'mean_temp': 3}]))
    results.pipeline.run()

  def test_basic_empty(self):
    """Test that the correct empty result is returned for a simple dataset."""
    results = self._get_result_for_month(3)
    beam.assert_that(results, beam.equal_to([]))
    results.pipeline.run()

  def test_basic_empty_missing(self):
    """Test that the correct empty result is returned for a missing month."""
    results = self._get_result_for_month(4)
    beam.assert_that(results, beam.equal_to([]))
    results.pipeline.run()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
