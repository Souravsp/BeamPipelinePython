

"""Test for the bigshuffle example."""

import logging
import tempfile
import unittest

from apache_beam.examples.cookbook import bigshuffle


class BigShuffleTest(unittest.TestCase):

  SAMPLE_TEXT = 'a b c a b a\naa bb cc aa bb aa'

  def create_temp_file(self, contents):
    with tempfile.NamedTemporaryFile(delete=False) as f:
      f.write(contents)
      return f.name

  def test_basics(self):
    temp_path = self.create_temp_file(self.SAMPLE_TEXT)
    bigshuffle.run([
        '--input=%s*' % temp_path,
        '--output=%s.result' % temp_path,
        '--checksum_output=%s.checksum' % temp_path]).wait_until_finish()
    # Parse result file and compare.
    results = []
    with open(temp_path + '.result-00000-of-00001') as result_file:
      for line in result_file:
        results.append(line.strip())
    expected = self.SAMPLE_TEXT.split('\n')
    self.assertEqual(sorted(results), sorted(expected))
    # Check the checksums
    input_csum = ''
    with open(temp_path + '.checksum-input-00000-of-00001') as input_csum_file:
      input_csum = input_csum_file.read().strip()
    output_csum = ''
    with open(temp_path +
              '.checksum-output-00000-of-00001') as output_csum_file:
      output_csum = output_csum_file.read().strip()
    expected_csum = 'd629c1f6'
    self.assertEqual(input_csum, expected_csum)
    self.assertEqual(input_csum, output_csum)

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
