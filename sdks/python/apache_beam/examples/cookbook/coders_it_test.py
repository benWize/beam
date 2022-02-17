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

"""End-to-end test for Coders example."""
# pytype: skip-file

import json
import logging
import unittest
import uuid

import pytest

from apache_beam.examples.cookbook import coders
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
    output.append(gcs.open(file_name).read().decode('utf-8').strip())
  return '\n'.join(output)


def create_content_input_file(path, contents):
  logging.info('Creating file: %s', path)
  gcs = gcsio.GcsIO()
  with gcs.open(path, 'w') as f:
    f.write(str.encode(contents, 'utf-8'))


def format_result(result_string):
  def format_tuple(result_elem_list):
    [country, counter] = result_elem_list
    return country, int(counter.strip())

  result_list = list(
      map(
          lambda result_elem: format_tuple(result_elem.split(',')),
          result_string.replace('\'', '').replace('[',
                                                  '').replace(']', '').replace(
                                                      '\"', '').split('\n')))
  return result_list


class CodersIT(unittest.TestCase):
  SAMPLE_RECORDS = [{
      'host': ['Germany', 1], 'guest': ['Italy', 0]
  }, {
      'host': ['Germany', 1], 'guest': ['Brasil', 3]
  }, {
      'host': ['Brasil', 1], 'guest': ['Italy', 0]
  }]

  EXPECTED_RESULT = [('Italy', 0), ('Brasil', 6), ('Germany', 3)]

  @pytest.mark.no_xdist
  @pytest.mark.examples_postcommit
  def test_coders_output_files_on_small_input(self):
    test_pipeline = TestPipeline(is_integration_test=True)
    # Setup the files with expected content.
    OUTPUT_FILE_DIR = \
        'gs://temp-storage-for-end-to-end-tests/py-it-cloud/output'
    output = '/'.join([OUTPUT_FILE_DIR, str(uuid.uuid4()), 'result'])
    INPUT_FILE_DIR = \
        'gs://temp-storage-for-end-to-end-tests/py-it-cloud/input'
    input = '/'.join([INPUT_FILE_DIR, str(uuid.uuid4()), 'input.txt'])
    create_content_input_file(
        input, '\n'.join(map(json.dumps, self.SAMPLE_RECORDS)))
    extra_opts = {'input': input, 'output': output}
    coders.run(test_pipeline.get_full_options_as_args(**extra_opts))

    # Load result file and compare.
    result = read_gcs_output_file(output).strip()

    self.assertEqual(
        sorted(self.EXPECTED_RESULT), sorted(format_result(result)))
