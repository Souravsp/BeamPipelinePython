

"""Test for the BigQuery side input example."""

import logging
import unittest

import apache_beam as beam
from apache_beam.examples.cookbook import bigquery_side_input
from apache_beam.test_pipeline import TestPipeline


class BigQuerySideInputTest(unittest.TestCase):

  def test_create_groups(self):
    p = TestPipeline()

    group_ids_pcoll = p | 'CreateGroupIds' >> beam.Create(['A', 'B', 'C'])
    corpus_pcoll = p | 'CreateCorpus' >> beam.Create(
        [{'f': 'corpus1'}, {'f': 'corpus2'}, {'f': 'corpus3'}])
    words_pcoll = p | 'CreateWords' >> beam.Create(
        [{'f': 'word1'}, {'f': 'word2'}, {'f': 'word3'}])
    ignore_corpus_pcoll = p | 'CreateIgnoreCorpus' >> beam.Create(['corpus1'])
    ignore_word_pcoll = p | 'CreateIgnoreWord' >> beam.Create(['word1'])

    groups = bigquery_side_input.create_groups(group_ids_pcoll, corpus_pcoll,
                                               words_pcoll, ignore_corpus_pcoll,
                                               ignore_word_pcoll)

    beam.assert_that(groups, beam.equal_to(
        [('A', 'corpus2', 'word2'),
         ('B', 'corpus2', 'word2'),
         ('C', 'corpus2', 'word2')]))
    p.run()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
