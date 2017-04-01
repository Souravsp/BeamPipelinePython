

"""Simple tests to showcase combiners.

The tests are meant to be "copy/paste" code snippets for the topic they address
(combiners in this case). Most examples use neither sources nor sinks.
The input data is generated simply with a Create transform and the output is
checked directly on the last PCollection produced.
"""

import logging
import unittest

import apache_beam as beam
from apache_beam.test_pipeline import TestPipeline


class CombinersTest(unittest.TestCase):
  """Tests showcasing Dataflow combiners."""

  SAMPLE_DATA = [
      ('a', 1), ('b', 10), ('a', 2), ('a', 3), ('b', 20), ('c', 100)]

  def test_combine_per_key_with_callable(self):
    """CombinePerKey using a standard callable reducing iterables.

    A common case for Dataflow combiners is to sum (or max or min) over the
    values of each key. Such standard functions can be used directly as combiner
    functions. In fact, any function "reducing" an iterable to a single value
    can be used.
    """
    result = (
        TestPipeline()
        | beam.Create(CombinersTest.SAMPLE_DATA)
        | beam.CombinePerKey(sum))

    beam.assert_that(result, beam.equal_to([('a', 6), ('b', 30), ('c', 100)]))
    result.pipeline.run()

  def test_combine_per_key_with_custom_callable(self):
    """CombinePerKey using a custom function reducing iterables."""
    def multiply(values):
      result = 1
      for v in values:
        result *= v
      return result

    result = (
        TestPipeline()
        | beam.Create(CombinersTest.SAMPLE_DATA)
        | beam.CombinePerKey(multiply))

    beam.assert_that(result, beam.equal_to([('a', 6), ('b', 200), ('c', 100)]))
    result.pipeline.run()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
