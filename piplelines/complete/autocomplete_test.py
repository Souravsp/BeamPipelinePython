

"""Test for the autocomplete example."""

import unittest

import apache_beam as beam
from apache_beam.examples.complete import autocomplete
from apache_beam.test_pipeline import TestPipeline
from apache_beam.transforms.util import assert_that
from apache_beam.transforms.util import equal_to


class AutocompleteTest(unittest.TestCase):

  WORDS = ['this', 'this', 'that', 'to', 'to', 'to']

  def test_top_prefixes(self):
    p = TestPipeline()
    words = p | beam.Create(self.WORDS)
    result = words | autocomplete.TopPerPrefix(5)
    # values must be hashable for now
    result = result | beam.Map(lambda (k, vs): (k, tuple(vs)))
    assert_that(result, equal_to(
        [
            ('t', ((3, 'to'), (2, 'this'), (1, 'that'))),
            ('to', ((3, 'to'), )),
            ('th', ((2, 'this'), (1, 'that'))),
            ('thi', ((2, 'this'), )),
            ('this', ((2, 'this'), )),
            ('tha', ((1, 'that'), )),
            ('that', ((1, 'that'), )),
        ]))
    p.run()

if __name__ == '__main__':
  unittest.main()
