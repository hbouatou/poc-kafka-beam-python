import os
import argparse

from tweepy import Stream

THIS_DIR = os.path.dirname(__file__)


def get_tweepy_auth():
    return {
        'access_token': os.environ.get('ACCESS_TOKEN'),
        'access_token_secret': os.environ.get('ACCESS_TOKEN_SECRET'),
        'consumer_key': os.environ.get('CONSUMER_KEY'),
        'consumer_secret': os.environ.get('CONSUMER_SECRET')
    }


class TwitterStreamProducer(Stream):

    def __init__(self, outfile, limit):
        super().__init__(**get_tweepy_auth())
        self.outfile = outfile
        self.limit = limit
        self.counter = 0

    def on_data(self, raw_data):
        print(self.counter, raw_data)
        if self.counter >= self.limit:
            self.disconnect()
            return False
        with open(self.outfile, 'a') as ft:
            ft.write(f'{raw_data.decode("utf-8")}\n')
            self.counter += 1
        return True

    def on_request_error(self, status_code):
        print(status_code)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--outfile',
        dest='outfile',
        default='fetched_tweets.txt',
        help='Fetched tweets output filename'
    )
    parser.add_argument(
        '--limit',
        dest='limit',
        default=10000,
        help='Max number of tweets written to the file'
    )
    args, _ = parser.parse_known_args()
    stream = TwitterStreamProducer(args.outfile, args.limit)
    stream.filter(track=['bitcoin', 'ethereum', 'solana', 'cardano', 'polkadot'], languages=['en'])
