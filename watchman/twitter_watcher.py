import logging
import requests
import pandas as pd
import time
from google.cloud import bigquery

logger = logging.getLogger(__name__)


class TwitterWatcher:
    """
    Class to get Tweets from Twitter.
    It enables to authenticate to the Twitter API and iteratively perform query and get results.
    Twitter Developer portal: https://developer.twitter.com/en/portal/dashboard

    Max 500.000 tweets/month
    Max 450 requests/15 min per app auth
    Max 180 requests/15 min per user auth
    """

    def __init__(self, bearer_token):
        self.bearer_token = bearer_token
        self.headers = {"Authorization": "Bearer {}".format(self.bearer_token)}
        self.url = None

    def _make_query(self, hashtags: list, start_time=None, end_time=None, max_results_per_page=100, next_token=None, since_id=None):
        """
        Make a hhtp query from a list of hashtags

        :param hashtags: (list) the list of hashtags.
        :param start_time: (str) the start time of the search. If not specified is 7 days ago.
        :param end_time: (str) the end time of the search. If not specified is 30 seconds ago.
        :param max_results_per_page: (int=100) max number of Tweets returned per response.
        :param next_token: (str) the token to include in the query to get the next page of results.
        :param since_id: (int) the id of the last tweet got.
        :return: No return.
        """

        logger.info("Making query from hashtags: {hashtags}".format(hashtags=hashtags))

        # Unicode: replace # with %23
        hashtags = [hashtag.replace('#', '%23') for hashtag in hashtags]
        # Concatenate hashtags
        query = '%20OR%20'.join(hashtags)
        # Define tweet fields
        tweet_fields = "tweet.fields=author_id,created_at,public_metrics,entities"
        # Define expansion & fields
        user_expansion = "expansions=author_id"
        user_fields = "user.fields=public_metrics"
        self.url = "https://api.twitter.com/2/tweets/search/recent?query={query}&{tweet_fields}&{user_expansion}&{user_fields}&max_results={max_results}".format(
            query=query, tweet_fields=tweet_fields, user_expansion=user_expansion, user_fields=user_fields, max_results=max_results_per_page)
        # Add start_time to the query if specified
        if start_time is not None:
            self.url = self.url + '&start_time={start_time}'.format(start_time=start_time)
        # Add end_time to the query if specified
        if end_time is not None:
            self.url = self.url + '&end_time={end_time}'.format(end_time=end_time)
        # Add next_token to the query if specified
        if next_token is not None:
            self.url = self.url + '&next_token={next_token}'.format(next_token=next_token)
        # Add since_id to the query if specified
        if since_id is not None:
            self.url = self.url + '&since_id={since_id}'.format(since_id=since_id)

        logger.info("Query made.")

    def _get_page_results(self):
        """
        Submit the url and get the results.

        :return: No return.
        """

        logger.info("Submitting the url: {url}".format(url=self.url))

        if self.url is not None:
            # Make http request
            response = requests.request("GET", self.url, headers=self.headers)
            logger.info(f'Twitter Response Status Code: {response.status_code}')
            if response.status_code != 200:
                raise Exception(response.status_code, response.text)
            # Return the results as pd.DataFrame and metadata as dict
            logger.info("Url submitted and results obtained.")
            return self._df_from_response(response), response.json()['meta']
        else:
            logger.warning('Please make a query before submitting.')

    def write_results(self, hashtags: list, start_time=None, end_time=None, max_results_per_page=100, max_results=15000, write_df_to_bq=False, bq_cred_path=None, bq_destination_table_id=None):
        """
        Iteratively make http query from a list of hashtags and get results.
        The results are then stored (and optionally sent to Google BigQuery) in a pd.DataFrame which is finally returned.

        :param hashtags: (list) the list of hashtags.
        :param start_time: (str) the start time of the search. If not specified is 7 days ago.
        :param end_time: (str) the end time of the search. If not specified is 30 seconds ago.
        :param max_results_per_page: (int=100) max number of Tweets returned per response.
        :param max_results: (int=15000) max number of daily Tweets returned.
        :param write_df_to_bq: (bool=False) if the result should be written/appended in a Google BigQuery table.
        :param bq_cred_path: (str=None) Google BigQuery credentials complete path.
        :param bq_destination_table_id: (str=None) Google BigQuery destination table id.
        :return: A pd.DataFrame with all the Tweets.
        """

        # Init DataFrame with all results
        df_results = pd.DataFrame()

        # Start with the first query
        self._make_query(hashtags=hashtags, max_results_per_page=max_results_per_page, start_time=start_time, end_time=end_time)
        df_page_results, meta_dict = self._get_page_results()
        df_results = df_results.append(df_page_results)

        # Iterate until 15000 tweets are reached or all pages are got
        while df_results.shape[0] < max_results and 'next_token' in meta_dict.keys():
            self._make_query(hashtags=hashtags, max_results_per_page=max_results_per_page, start_time=start_time, end_time=end_time, next_token=meta_dict['next_token'])
            df_page_results, meta_dict = self._get_page_results()
            df_results = df_results.append(df_page_results)

            logger.info("Tweets count: {tweets_count}".format(tweets_count=df_results.shape[0]))

            # Sleep 5 seconds
            time.sleep(5)

        # Set index
        df_results.set_index('tweet_id', 'tweet_id_seq', inplace=True)

        # Write to Google BigQuery
        if write_df_to_bq:
            logger.info(f"Start to write the result (nrows: {df_results.shape[0]} - ncols: {df_results.shape[1]}) to Google BigQuery table {bq_destination_table_id}...")
            job_status = self._write_df_to_bigquery(df=df_results, bq_cred_path=bq_cred_path, bq_destination_table_id=bq_destination_table_id)
            if job_status != 'DONE':
                raise Exception(f"Error: Google BigQuery Job status: {job_status}")
            else:
                logger.info("Result successfully written to Google BigQuery.")

        return df_results

    @staticmethod
    def _df_from_response(res: requests.Response):
        """
        Static method to convert http response in pandas Dataframe.

        :param res: The response to convert in pandas DataFrame
        :return: Response converted as pandas DataFrame
        """

        # Tweet info format
        df = pd.DataFrame(res.json()['data'])
        # Extract cashtags
        df['cashtags'] = df['entities'].apply(lambda x: [cashtag['tag'] for cashtag in x['cashtags']] if 'cashtags' in x.keys() else [])
        # Unpack tweet public metrics
        df['retweet_count'], df['reply_count'], df['like_count'], df['quote_count'] = zip(
            *df['public_metrics'].apply(lambda x: [x['retweet_count'], x['reply_count'], x['like_count'], x['quote_count']]))
        # Remove unpacked column
        df.drop(['public_metrics'], axis=1, inplace=True)
        # Rename columns
        df.rename(columns={'id': 'tweet_id'}, inplace=True)

        # User info format
        df_user = pd.DataFrame(res.json()['includes']['users'])
        # Unpack user public metrics
        df_user['followers_count'], df_user['following_count'], df_user['tweet_count'], df_user['listed_count'] = zip(
            *df_user['public_metrics'].apply(lambda x: [x['followers_count'], x['following_count'], x['tweet_count'], x['listed_count']]))
        # Remove unpacked column
        df_user.drop(['public_metrics'], axis=1, inplace=True)

        # Merge info and return out
        out = pd.merge(left=df, right=df_user, left_on='author_id', right_on='id', how='left')
        # Drop redundant author id column
        out.drop(['id'], axis=1, inplace=True)

        return out

    @staticmethod
    def _write_df_to_bigquery(df: pd.DataFrame, bq_cred_path: str, bq_destination_table_id: str):
        """
        Static method to write a Pandas.DataFrame in a Google BigQuery table.

        :param df: The Pandas.DataFrame to write.
        :param bq_cred_path: Google BigQuery credentials complete path.
        :param bq_destination_table_id: Google BigQuery destination table id.
        :return: The Job Status (str).
        """

        # Construct a BigQuery client object.
        if bq_cred_path:
            client = bigquery.Client.from_service_account_json(bq_cred_path)
        else:
            # Logged with the service account which invoke App Engine
            client = bigquery.Client

        # Define destinantion table id
        table_id = bq_destination_table_id

        # Define Job Configuration
        job_config = bigquery.LoadJobConfig(
            # Specify a (partial) schema. All columns are always written to the table.
            # The schema is used to assist in data type definitions for column whose type cannot be auto-detected.
            schema=[
                bigquery.SchemaField("tweet_id", bigquery.enums.SqlTypeNames.STRING),
                # bigquery.SchemaField("entities", bigquery.enums.SqlTypeNames.RECORD), TODO: Definire lo schema senza farlo dedurre a pyarrow
                bigquery.SchemaField("author_id", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("created_at", bigquery.enums.SqlTypeNames.DATETIME),
                bigquery.SchemaField("text", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("cashtags", bigquery.enums.SqlTypeNames.RECORD),
                bigquery.SchemaField("reply_count", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("like_count", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("quote_count", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("username", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("name", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("followers_count", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("following_count", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("tweet_count", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("listed_count", bigquery.enums.SqlTypeNames.INTEGER),
            ],
            # Optionally, set the write disposition. BigQuery appends loaded rows
            # to an existing table by default, but with WRITE_TRUNCATE write
            # disposition it replaces the table with the loaded data.
            write_disposition="WRITE_APPEND",
        )

        # Make an API request.
        job = client.load_table_from_dataframe(dataframe=df, destination=table_id, job_config=job_config)

        return job.result().state
