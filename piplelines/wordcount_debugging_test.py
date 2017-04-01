"""Test for the debugging wordcount example."""

import logging
import re
import tempfile
import unittest

from apache_beam.examples import wordcount_debugging


class WordCountTest(unittest.TestCase):
    SAMPLE_TEXT = 'xx yy Flourish\n zz Flourish Flourish stomach\n aa\n bb cc dd'

    def create_temp_file(self, contents):
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(contents)
            return f.name

    def get_results(self, temp_path):
        results = []
        with open(temp_path + '.result-00000-of-00001') as result_file:
            for line in result_file:
                match = re.search(r'([A-Za-z]+): ([0-9]+)', line)
                if match is not None:
                    results.append((match.group(1), int(match.group(2))))
        return results

    def test_basics(self):
        temp_path = self.create_temp_file(self.SAMPLE_TEXT)
        expected_words = [('Flourish', 3), ('stomach', 1)]
        wordcount_debugging.run([
            '--input=%s*' % temp_path,
            '--output=%s.result' % temp_path])

        # Parse result file and compare.
        results = self.get_results(temp_path)
        self.assertEqual(sorted(results), sorted(expected_words))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    unittest.main()
