

"""A Julia set computing workflow: https://en.wikipedia.org/wiki/Julia_set.

This example has in the juliaset/ folder all the code needed to execute the
workflow. It is organized in this way so that it can be packaged as a Python
package and later installed in the VM workers executing the job. The root
directory for the example contains just a "driver" script to launch the job
and the setup.py file needed to create a package.

The advantages for organizing the code is that large projects will naturally
evolve beyond just one module and you will have to make sure the additional
modules are present in the worker.

In Python Dataflow, using the --setup_file option when submitting a job, will
trigger creating a source distribution (as if running python setup.py sdist) and
then staging the resulting tarball in the staging area. The workers, upon
startup, will install the tarball.

Below is a complete command line for running the juliaset workflow remotely as
an example:

python juliaset_main.py \
  --job_name juliaset-$USER \
  --project YOUR-PROJECT \
  --runner DataflowRunner \
  --setup_file ./setup.py \
  --staging_location gs://YOUR-BUCKET/juliaset/staging \
  --temp_location gs://YOUR-BUCKET/juliaset/temp \
  --coordinate_output gs://YOUR-BUCKET/juliaset/out \
  --grid_size 20 \

"""

import logging


from apache_beam.examples.complete.juliaset.juliaset import juliaset


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  juliaset.run()
