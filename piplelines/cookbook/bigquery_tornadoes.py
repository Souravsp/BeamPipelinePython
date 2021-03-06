

"""A workflow using BigQuery sources and sinks.

The workflow will read from a table that has the 'month' and 'tornado' fields as
part of the table schema (other additional fields are ignored). The 'month'
field is a number represented as a string (e.g., '23') and the 'tornado' field
is a boolean field.

The workflow will compute the number of tornadoes in each month and output
the results to a table (created if needed) with the following schema:

- month: number
- tornado_count: number

This example uses the default behavior for BigQuery source and sinks that
represents table rows as plain Python dictionaries.
"""

from __future__ import absolute_import

import argparse
import logging

import apache_beam as beam


def count_tornadoes(input_data):
  """Workflow computing the number of tornadoes for each month that had one.

  Args:
    input_data: a PCollection of dictionaries representing table rows. Each
      dictionary will have a 'month' and a 'tornado' key as described in the
      module comment.

  Returns:
    A PCollection of dictionaries containing 'month' and 'tornado_count' keys.
    Months without tornadoes are skipped.
  """

  return (input_data
          | 'months with tornadoes' >> beam.FlatMap(
              lambda row: [(int(row['month']), 1)] if row['tornado'] else [])
          | 'monthly count' >> beam.CombinePerKey(sum)
          | 'format' >> beam.Map(
              lambda (k, v): {'month': k, 'tornado_count': v}))


def run(argv=None):
  parser = argparse.ArgumentParser()
  parser.add_argument('--input',
                      default='clouddataflow-readonly:samples.weather_stations',
                      help=('Input BigQuery table to process specified as: '
                            'PROJECT:DATASET.TABLE or DATASET.TABLE.'))
  parser.add_argument(
      '--output',
      required=True,
      help=
      ('Output BigQuery table for results specified as: PROJECT:DATASET.TABLE '
       'or DATASET.TABLE.'))
  known_args, pipeline_args = parser.parse_known_args(argv)

  p = beam.Pipeline(argv=pipeline_args)

  # Read the table rows into a PCollection.
  rows = p | 'read' >> beam.io.Read(beam.io.BigQuerySource(known_args.input))
  counts = count_tornadoes(rows)

  # Write the output using a "Write" transform that has side effects.
  # pylint: disable=expression-not-assigned
  counts | 'write' >> beam.io.Write(
      beam.io.BigQuerySink(
          known_args.output,
          schema='month:INTEGER, tornado_count:INTEGER',
          create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
          write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))

  # Run the pipeline (all operations are deferred until run() is called).
  p.run().wait_until_finish()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
