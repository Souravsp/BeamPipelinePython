"""Test for the minimal wordcount example."""

import collections
import logging
import re
import tempfile
import unittest

from apache_beam.examples import wordcount_minimal


class WordCountMinimalTest(unittest.TestCase):
    """Unit test for wordcount_minimal example with direct runner."""

    SAMPLE_TEXT = 'a b c a b a\n aa bb cc aa bb aa'

    def create_temp_file(self, contents):
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(contents)
            return f.name

    def test_basics(self):
        temp_path = self.create_temp_file(self.SAMPLE_TEXT)
        expected_words = collections.defaultdict(int)
        for word in re.findall(r'\w+', self.SAMPLE_TEXT):
            expected_words[word] += 1
        wordcount_minimal.run([
            '--input=%s*' % temp_path,
            '--output=%s.result' % temp_path])
        # Parse result file and compare.
        results = []
        with open(temp_path + '.result-00000-of-00001') as result_file:
            for line in result_file:
                match = re.search(r'([a-z]+): ([0-9]+)', line)
                if match is not None:
                    results.append((match.group(1), int(match.group(2))))
        self.assertEqual(sorted(results), sorted(expected_words.iteritems()))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    unittest.main()
