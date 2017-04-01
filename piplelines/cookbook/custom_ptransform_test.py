

"""Tests for the various custom Count implementation examples."""

import logging
import unittest

import apache_beam as beam
from apache_beam.examples.cookbook import custom_ptransform
from apache_beam.test_pipeline import TestPipeline
from apache_beam.transforms.util import assert_that
from apache_beam.transforms.util import equal_to


class CustomCountTest(unittest.TestCase):

  def test_count1(self):
    self.run_pipeline(custom_ptransform.Count1())

  def test_count2(self):
    self.run_pipeline(custom_ptransform.Count2())

  def test_count3(self):
    factor = 2
    self.run_pipeline(custom_ptransform.Count3(factor), factor=factor)

  def run_pipeline(self, count_implementation, factor=1):
    p = TestPipeline()
    words = p | beam.Create(['CAT', 'DOG', 'CAT', 'CAT', 'DOG'])
    result = words | count_implementation
    assert_that(
        result, equal_to([('CAT', (3 * factor)), ('DOG', (2 * factor))]))
    p.run()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
