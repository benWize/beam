#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""End-to-end test for Top Wikipedia Sessions example."""
# pytype: skip-file

import logging
import unittest
import uuid

import pytest

from apache_beam.examples.complete import top_wikipedia_sessions
from apache_beam.testing.test_pipeline import TestPipeline

# Protect against environments where gcsio library is not available.
try:
  from apache_beam.io.gcp import gcsio
except ImportError:
  gcsio = None


def read_gcs_output_file(file_pattern):
  gcs = gcsio.GcsIO()
  file_names = gcs.list_prefix(file_pattern).keys()
  output = []
  for file_name in file_names:
    output.append(gcs.open(file_name).read().decode('utf-8'))
  return '\n'.join(output)


def create_content_input_file(path, contents):
  logging.info('Creating file: %s', path)
  gcs = gcsio.GcsIO()
  with gcs.open(path, 'w') as f:
    f.write(str.encode(contents, 'utf-8'))


class ComputeTopSessionsIT(unittest.TestCase):
  # TODO Enable when fixed this tests for Dataflow runner
  @pytest.mark.sickbay_dataflow
  @pytest.mark.no_xdist
  @pytest.mark.examples_postcommit
  def test_top_wikipedia_sessions_output_files_on_small_input(self):
    test_pipeline = TestPipeline(is_integration_test=True)
    # Setup the files with expected content.
    OUTPUT_FILE_DIR = \
        'gs://temp-storage-for-end-to-end-tests/py-it-cloud/output'
    output = '/'.join([OUTPUT_FILE_DIR, str(uuid.uuid4()), 'result'])
    INPUT_FILE_DIR = \
        'gs://temp-storage-for-end-to-end-tests/py-it-cloud/input'
    input = '/'.join([INPUT_FILE_DIR, str(uuid.uuid4()), 'input.txt'])
    create_content_input_file(input, '\n'.join(self.EDITS))
    extra_opts = {'input': input, 'output': output, 'sampling_threshold': '1.0'}
    top_wikipedia_sessions.run(
        test_pipeline.get_full_options_as_args(**extra_opts))

    # Load result file and compare.
    result = read_gcs_output_file(output).strip().splitlines()

    self.assertEqual(self.EXPECTED, sorted(result, key=lambda x: x.split()[0]))
