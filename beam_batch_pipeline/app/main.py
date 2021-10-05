import os
import json
import argparse

import apache_beam as beam
from apache_beam.io import ReadFromText


from apache_beam.options.pipeline_options import PipelineOptions

from core.utils import ExtractWordsDoFn
from core.beam_kafka_io import KafkaProduce

THIS_DIR = os.path.dirname(__file__)


class BeamBatchProducer:

    def __init__(self, args, options):
        self.args = args
        self.options = options

    def run(self):
        with beam.Pipeline(options=self.options) as pipeline:
            _ = (pipeline
                 | 'Read from text' >> ReadFromText(self.args.filename)
                 | 'Split' >> (beam.ParDo(ExtractWordsDoFn()).with_output_types(str))
                 | 'Pair with one' >> beam.Map(lambda x: (x, 1))
                 | 'Group and sum' >> beam.CombinePerKey(sum)
                 | 'Filter low frequency words' >> beam.Filter(lambda element: element[1] > 100)
                 | 'convert dict to a byte string' >> beam.Map(lambda x: json.dumps(x).encode())
                 | 'Write to Kafka' >> KafkaProduce(
                        topic=self.args.topic, servers=self.args.bootstrap_servers)
                 # | 'Print' >> beam.Map(print)  # TODO: remove, for debugging only
                 )


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--bootstrap_servers',
        dest='bootstrap_servers',
        default=os.environ.get('BOOTSTRAP_SERVERS', 'localhost:9092'),
        help='Bootstrap servers for the Kafka'
    )
    parser.add_argument(
        '--topic',
        dest='topic',
        default='tweets-word-count',
        help='Kafka topic to write to'
    )
    parser.add_argument(
        '--filename',
        dest='filename',
        default=os.path.join(THIS_DIR, 'fetched_tweets.txt'),
        help='Fetched tweets filename'
    )
    known_args, pipeline_args = parser.parse_known_args()
    pipeline_options = PipelineOptions(pipeline_args, save_main_session=True)
    BeamBatchProducer(known_args, pipeline_options).run()
