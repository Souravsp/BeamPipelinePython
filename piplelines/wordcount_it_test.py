"""End-to-end test for the wordcount example."""

import logging
import unittest

from apache_beam.examples import wordcount
from apache_beam.test_pipeline import TestPipeline
from apache_beam.tests.pipeline_verifiers import FileChecksumMatcher
from apache_beam.tests.pipeline_verifiers import PipelineStateMatcher
from hamcrest.core.core.allof import all_of
from nose.plugins.attrib import attr


class WordCountIT(unittest.TestCase):
    # The default checksum is a SHA-1 hash generated from a sorted list of
    # lines read from expected output.
    DEFAULT_CHECKSUM = '33535a832b7db6d78389759577d4ff495980b9c0'

    @attr('IT')
    def test_wordcount_it(self):
        test_pipeline = TestPipeline(is_integration_test=True)

        # Set extra options to the pipeline for test purpose
        output = '/'.join([test_pipeline.get_option('output'),
                           test_pipeline.get_option('job_name'),
                           'results'])
        arg_sleep_secs = test_pipeline.get_option('sleep_secs')
        sleep_secs = int(arg_sleep_secs) if arg_sleep_secs is not None else None
        pipeline_verifiers = [PipelineStateMatcher(),
                              FileChecksumMatcher(output + '*-of-*',
                                                  self.DEFAULT_CHECKSUM,
                                                  sleep_secs)]
        extra_opts = {'output': output,
                      'on_success_matcher': all_of(*pipeline_verifiers)}

        # Get pipeline options from command argument: --test-pipeline-options,
        # and start pipeline job by calling pipeline main function.
        wordcount.run(test_pipeline.get_full_options_as_args(**extra_opts))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.DEBUG)
    unittest.main()
