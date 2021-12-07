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

"""Test for the BigQuery side input example."""

# pytype: skip-file

import logging
import os
import tempfile
import unittest

import pytest

import apache_beam as beam
from apache_beam.examples.cookbook import bigquery_side_input
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.testing.util import open_shards


class BigQuerySideInputTest(unittest.TestCase):
  def test_create_groups(self):
    with TestPipeline() as p:

      group_ids_pcoll = p | 'CreateGroupIds' >> beam.Create(['A', 'B', 'C'])
      corpus_pcoll = p | 'CreateCorpus' >> beam.Create([{
          'f': 'corpus1'
      }, {
          'f': 'corpus2'
      }])
      words_pcoll = p | 'CreateWords' >> beam.Create([{
          'f': 'word1'
      }, {
          'f': 'word2'
      }])
      ignore_corpus_pcoll = p | 'CreateIgnoreCorpus' >> beam.Create(['corpus1'])
      ignore_word_pcoll = p | 'CreateIgnoreWord' >> beam.Create(['word1'])

      groups = bigquery_side_input.create_groups(
          group_ids_pcoll,
          corpus_pcoll,
          words_pcoll,
          ignore_corpus_pcoll,
          ignore_word_pcoll)

      assert_that(
          groups,
          equal_to([('A', 'corpus2', 'word2'), ('B', 'corpus2', 'word2'),
                    ('C', 'corpus2', 'word2')]))

  @pytest.mark.examples_postcommit
  def test_basics(self):
    test_pipeline = TestPipeline(is_integration_test=True)
    NUM_GROUPS = 3

    # Setup the files with expected content.
    temp_folder = tempfile.mkdtemp()

    extra_opts = {
        'output': os.path.join(temp_folder, 'result'),
        'num_groups': str(NUM_GROUPS)
    }

    bigquery_side_input.run(
        test_pipeline.get_full_options_as_args(**extra_opts),
        save_main_session=False)

    # Load result file and compare.
    with open_shards(os.path.join(temp_folder, 'result-*-of-*')) as result_file:
      result = result_file.read().strip().splitlines()

    self.assertEqual(len(result), NUM_GROUPS)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
