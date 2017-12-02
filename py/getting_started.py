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

from __future__ import absolute_import

import argparse
import csv
import logging

import apache_beam as beam
from apache_beam.metrics.metric import Metrics
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


class ParseGameEventFn(beam.DoFn):
  """Parses the raw game event info into a Python dictionary.

  Each event line has the following format:
    username,teamname,score,timestamp_in_ms,readable_time

  e.g.:
    user2_AsparagusPig,AsparagusPig,10,1445230923951,2015-11-02 09:09:28.224

  The human-readable time string is not used here.
  """
  def __init__(self):
    super(ParseGameEventFn, self).__init__()

  def process(self, elem):
    try:
      row = elem.split(',') 
      yield {
          'user': row[0],
          'team': row[1],
          'score': int(row[2]),
          'timestamp': int(row[3]) / 1000.0,
      }
    except:
      # Log and count parse errors
      logging.error('Parse error on "%s"', elem)


def run(argv=None):
  """Main entry point; defines and runs the user_score pipeline."""
  parser = argparse.ArgumentParser()

  # The default maps to two large Google Cloud Storage files (each ~12GB)
  # holding two subsequent day's worth (roughly) of data.
  parser.add_argument('--input',
                      type=str,
                      default='gs://apache-beam-samples/game/gaming_data*.csv',
                      help='Path to the data file(s) containing game data.')
  parser.add_argument('--output',
                      type=str,
                      required=True,
                      help='Path to the output file(s).')

  args, pipeline_args = parser.parse_known_args(argv)

  options = PipelineOptions(pipeline_args)

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  options.view_as(SetupOptions).save_main_session = True

  with beam.Pipeline(options=options) as p:
    input_records = (p
     | 'ReadInputText' >> beam.io.ReadFromText(args.input))

    # Parse the comma-separated values into a dictionary
    parsed_records = (input_records 
                       | 'ParseGameEventFn' >> beam.ParDo(ParseGameEventFn()))

    # Extract username/score pairs from the event data.

    # Group score-user by key
    # Sum the score for every user.

    # Format the results and write down to files.
    (total_user_scores
     | 'FormatUserScoreSums' >> beam.Map(str)
     | 'WriteUserScoreSums' >> beam.io.WriteToText(args.output))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
