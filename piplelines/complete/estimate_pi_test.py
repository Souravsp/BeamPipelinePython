

"""Test for the estimate_pi example."""

import logging
import unittest

from apache_beam.examples.complete import estimate_pi
from apache_beam.test_pipeline import TestPipeline
from apache_beam.transforms.util import assert_that
from apache_beam.transforms.util import BeamAssertException


def in_between(lower, upper):
  def _in_between(actual):
    _, _, estimate = actual[0]
    if estimate < lower or estimate > upper:
      raise BeamAssertException(
          'Failed assert: %f not in [%f, %f]' % (estimate, lower, upper))
  return _in_between


class EstimatePiTest(unittest.TestCase):

  def test_basics(self):
    p = TestPipeline()
    result = p | 'Estimate' >> estimate_pi.EstimatePiTransform(5000)

    # Note: Probabilistically speaking this test can fail with a probability
    # that is very small (VERY) given that we run at least 500 thousand trials.
    assert_that(result, in_between(3.125, 3.155))
    p.run()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
