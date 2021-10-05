import nltk
import re
import json
import apache_beam as beam

from nltk.corpus import stopwords

nltk.download('stopwords')
STOP_WORDS = set(stopwords.words('english'))


class ExtractWordsDoFn(beam.DoFn):

    def process(self, element):
        text = json.loads(element)['text']
        text = text.replace("RT", "").lower().strip()
        text = " ".join(re.sub("([^0-9A-Za-z \t])|(\w+:\/\/\S+)", "", text).split())
        return [word for word in text.split() if word not in STOP_WORDS]


def sort_data(element):
    key, data = element
    sorted_data = sorted(data, key=lambda x: x[1], reverse=True)
    return sorted_data


def format_element(element):
    word, count = element
    return json.dumps({'word': word, 'count': count}).encode()
