"""A streaming string-capitalization workflow.

Important: streaming pipeline support in Python Dataflow is in development
and is not yet available for use.
"""

from __future__ import absolute_import

import argparse
import logging

import apache_beam as beam


def run(argv=None):
    """Build and run the pipeline."""

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input_topic', dest='input_topic', required=True,
        help='Input PubSub topic of the form "/topics/<PROJECT>/<TOPIC>".')
    parser.add_argument(
        '--output_topic', dest='output_topic', required=True,
        help='Output PubSub topic of the form "/topics/<PROJECT>/<TOPIC>".')
    known_args, pipeline_args = parser.parse_known_args(argv)

    p = beam.Pipeline(argv=pipeline_args)

    # Read the text file[pattern] into a PCollection.
    lines = p | beam.io.Read(
        beam.io.PubSubSource(known_args.input_topic))

    # Capitalize the characters in each line.
    transformed = (lines
                   | 'capitalize' >> (beam.Map(lambda x: x.upper())))

    # Write to PubSub.
    # pylint: disable=expression-not-assigned
    transformed | beam.io.Write(
        beam.io.PubSubSink(known_args.output_topic))

    p.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
