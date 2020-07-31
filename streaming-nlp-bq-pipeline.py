# Copyright 2020 Google LLC.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# [START SAF pubsub_to_bigquery]

import argparse
import logging
import re
import json
import time
from ast import literal_eval

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions


# Process JSON Input

class ProcessJSON(beam.DoFn):

    def process(self, data):

        # get the column with text      
        processjson_output = literal_eval(data)
        text = processjson_output['comments']

        # clean up the text a bit
        text = re.sub('\n', ' ', text)
        text = re.sub(" +"," ",text).strip() 

        # Construct the dictionary to output
        processjson_output['text_to_analyze'] = text

        # Fix the editted field which can be a boolean or float
        processjson_output['edited'] = str(processjson_output['edited'])

        return [processjson_output]


# function to get NLP Sentiment and Entity

class GetNLPOutput(beam.DoFn):

    def process(self, processjson_output):

        #mimic the schema of saf
        get_nlp_output_response = processjson_output
        get_nlp_output_response['sentences'] = []
        get_nlp_output_response['entities'] = []

        from oauth2client.client import GoogleCredentials
        from googleapiclient import discovery
        credentials = GoogleCredentials.get_application_default()
        nlp_service = discovery.build('language', 'v1beta2', credentials=credentials)

        # [START NLP analyzeSentiment]
        get_operation_sentiment = nlp_service.documents().analyzeSentiment(
            body={
                'document': {
                    'type': 'PLAIN_TEXT',
                    'content': processjson_output['text_to_analyze']
                }
            })
        response_sentiment = get_operation_sentiment.execute()

        get_nlp_output_response['sentimentscore'] = response_sentiment['documentSentiment']['score']
        get_nlp_output_response['magnitude'] = response_sentiment['documentSentiment']['magnitude']

        # [END NLP analyzeSentiment]


        # [START NLP analyzeEntitySentiment]
        get_operation_entity = nlp_service.documents().analyzeEntitySentiment(
            body={
                'document': {
                    'type': 'PLAIN_TEXT',
                    'content': processjson_output['text_to_analyze']
                }
            })
        response_entity = get_operation_entity.execute()

        for element in response_entity['entities']:
            get_nlp_output_response['entities'].append({
                'name': element['name'],
                'type': element['type'],
                'sentiment': element['sentiment']['score']
            })
        # [END NLP analyzeEntitySentiment]

        # print (get_nlp_output_response)

        # handle API Rate Limit for NLP
        time.sleep(3)

        return [get_nlp_output_response]


def run(argv=None, save_main_session=True):
    """The main function which creates the pipeline and runs it."""

    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)

    group.add_argument(
        '--input_topic',
        help=('Input PubSub topic of the form '
              '"projects/<PROJECT>/topics/<TOPIC>".'))
    group.add_argument(
        '--input_subscription',
        help=('Input PubSub subscription of the form '
              '"projects/<PROJECT>/subscriptions/<SUBSCRIPTION>."'))

    parser.add_argument('--output_bigquery', required=True,
                        help='Output BQ table to write results to '
                             '"PROJECT_ID:DATASET.TABLE"')

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    pipeline_options.view_as(StandardOptions).streaming = True
    
    p = beam.Pipeline(options=pipeline_options)

    # Read from PubSub into a PCollection.
    if known_args.input_subscription:
        messages = (p
                    | beam.io.ReadFromPubSub(
                    subscription=known_args.input_subscription)
                    .with_output_types(bytes))
    else:
        messages = (p
                    | beam.io.ReadFromPubSub(topic=known_args.input_topic)
                    .with_output_types(bytes))

    decode_messages = messages | 'DecodePubSubMessages' >> beam.Map(lambda x: x.decode('utf-8'))

    print (decode_messages)
    # Process the JSON file
    processjson_output = decode_messages | 'ProcessJSON' >> beam.ParDo(ProcessJSON())

    # Get NLP Sentiment and Entity response
    nlp_output = processjson_output | 'NaturalLanguageOutput' >> beam.ParDo(GetNLPOutput())

    # Write to BigQuery
    bigquery_table_schema = {
        "fields": [
        {
            "mode": "NULLABLE", 
            "name":"timestamp",
            "type":"string"
         },
         {
            "mode": "NULLABLE", 
            "name":"distinguished",
            "type":"string"
         },
         {
            "mode": "NULLABLE",
            "name":"edited",
            "type":"string"
         },
         {
            "mode": "NULLABLE",
            "name":"id",
            "type":"string"
         },
         {
            "mode": "NULLABLE",
            "name":"is_original_content",
            "type":"bool"
         },
         {
            "mode": "NULLABLE",
            "name":"selfpost",
            "type":"bool"
         },
         {
            "mode": "NULLABLE",
            "name":"link_flair_text",
            "type":"string"
         },
         {
            "mode": "NULLABLE",
            "name":"locked",
            "type":"bool"
         },
         {
            "mode": "NULLABLE",
            "name":"name",
            "type":"string"
         },
         {
            "mode": "NULLABLE",
            "name":"num_comments",
            "type":"integer"
         },
         {
            "mode": "NULLABLE",
            "name":"over_18",
            "type":"bool"
         },
         {
            "mode": "NULLABLE",
            "name":"permalink",
            "type":"string"
         },
         {
            "mode": "NULLABLE",
            "name":"num_upvotes",
            "type":"integer"
         },
         {
            "mode": "NULLABLE",
            "name":"selftext",
            "type":"string"
         },
         {
            "mode": "NULLABLE",
            "name":"spoiler",
            "type":"bool"
         },
         {
            "mode": "NULLABLE",
            "name":"stickied",
            "type":"bool"
         },
         {
            "mode": "NULLABLE",
            "name":"subreddit",
            "type":"string"
         },
         {
            "mode": "NULLABLE",
            "name":"title",
            "type":"string"
         },
         {
            "mode": "NULLABLE",
            "name":"upvote_ratio",
            "type":"float"
         },
         {
            "mode": "NULLABLE",
            "name":"url",
            "type":"string"
         },
         {
            "mode": "NULLABLE",
            "name":"comments",
            "type":"string"
         },
        {
            "mode": "NULLABLE",
            "name":"text_to_analyze",
            "type":"STRING"
         },
        {
            "mode": "NULLABLE", 
            "name": "sentimentscore", 
            "type": "FLOAT"
        },
        {
            "mode": "NULLABLE", 
            "name": "magnitude", 
            "type": "FLOAT"
        }, 
        {
            "fields": [
            {
                "mode": "NULLABLE", 
                "name": "name", 
                "type": "STRING"
            }, 
            {
                "mode": "NULLABLE", 
                "name": "type", 
                "type": "STRING"
            }, 
            {
                "mode": "NULLABLE", 
                "name": "sentiment", 
                "type": "FLOAT"
            }
            ], 
            "mode": "REPEATED", 
            "name": "entities", 
            "type": "RECORD"
        }, 
        {
            "fields": [
            {
                "mode": "NULLABLE", 
                "name": "sentence", 
                "type": "STRING"
            },
            {
                "mode": "NULLABLE", 
                "name": "sentiment", 
                "type": "FLOAT"
            }, 
            {
                "mode": "NULLABLE", 
                "name": "magnitude", 
                "type": "FLOAT"
            }
            ], 
            "mode": "REPEATED", 
            "name": "sentences", 
            "type": "RECORD"
            }
        ]
    }
     
    nlp_output | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
             known_args.output_bigquery,
             schema=bigquery_table_schema,
             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
             write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)

    #p.run().wait_until_finish()
    p.run()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.DEBUG)
    run()
# [END SAF pubsub_to_bigquery]



