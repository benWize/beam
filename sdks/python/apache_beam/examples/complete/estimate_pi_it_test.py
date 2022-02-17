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

"""End-to-end test for Estimate Pi example."""
# pytype: skip-file

import json
import unittest
import uuid

import pytest

from apache_beam.examples.complete import estimate_pi
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


class EstimatePiIT(unittest.TestCase):
  @pytest.mark.no_xdist
  @pytest.mark.examples_postcommit
  def test_estimate_pi_output_file(self):
    test_pipeline = TestPipeline(is_integration_test=True)
    OUTPUT_FILE = \
        'gs://temp-storage-for-end-to-end-tests/py-it-cloud/output'
    output = '/'.join([OUTPUT_FILE, str(uuid.uuid4()), 'result'])
    extra_opts = {'output': output}
    estimate_pi.run(test_pipeline.get_full_options_as_args(**extra_opts))
    # Load result file and compare.
    result = read_gcs_output_file(output)
    [_, _, estimated_pi] = json.loads(result.strip())
    # Note: Probabilistically speaking this test can fail with a probability
    # that is very small (VERY) given that we run at least 100 thousand
    # trials.
    self.assertTrue(3.125 <= estimated_pi <= 3.155)
