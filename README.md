# Stream Processing
The purpose of this project is to enrich a Kinesis data stream with an external dataset. This project is a build/upgrade on web scraping.

__The following were achieved:__
* Create a *shard kinesis stream*
* Create a *producer* to push results from web scraping into the created *kinesis* every 30 seconds
* Create a  *consumer* to collect data from the *kinesis stream*
* Enrinch the data consumed
* Append to CSV

## Configuration
* AWS
* boto3
* pandas
* requests
* beautifulsoup
