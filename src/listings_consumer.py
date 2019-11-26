import datetime as dt
import json
import time

import boto3
import pandas as pd


kinesis = boto3.client('kinesis', region_name='us-east-2')


def get_shard_iterator(stream_name, shard_id, iterator_type, date):
    shard_response = kinesis.get_shard_iterator(
        StreamName=stream_name, 
        ShardId=shard_id, 
        ShardIteratorType=iterator_type, 
        Timestamp=date
    )

    return shard_response['ShardIterator']



def listings_consumer(next_shard_iterator):
    start = dt.datetime.now()
    finish = start + dt.timedelta(seconds=60)
    
    # we'll only conusme for 60 seconds
    while start < finish:
        response = kinesis.get_records(
            ShardIterator=next_shard_iterator,
            Limit=20
        )
        
        try:
            data = response['Records'][0]['Data']
            data = json.loads(data)

            df = pd.DataFrame(data)   
            print(df)
        except IndexError:
            # this error occurs because we have consumed all the data available in the shard
            print("No new records have arrived")

        # get the next shard iterator
        next_shard_iterator = response['NextShardIterator']
        
        # pause checking for new data every 5 seconds
        time.sleep(5)



if __name__ == '__main__':
    stream_name = 'real_estate'
    shard_id = 'shardId-000000000000'
    iterator_type = 'AT_TIMESTAMP'
    date = dt.datetime.today().date().__str__()
    
    next_shard_iterator = get_shard_iterator(stream_name, shard_id, iterator_type, date)
    listings_consumer(next_shard_iterator)
    