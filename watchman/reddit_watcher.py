import requests
import pandas as pd
from datetime import datetime
import logging
from google.cloud import bigquery

logger = logging.getLogger(__name__)


class RedditWatcher:

    def __init__(self, personal_use_script: str, token: str, username: str, password: str):
        """
        RedditWatcher init method.
        It performs the authentication.

        Docs:
        https://towardsdatascience.com/how-to-use-the-reddit-api-in-python-5e05ddfd1e5c
        https://www.reddit.com/dev/api/#GET_new

        :param personal_use_script: (str) reddit api personal use script.
        :param token: (str) reddit api token.
        :param username: (str) reddit username.
        :param password: (str) reddit password.
        """
        # Attributes
        self.personal_use_script = personal_use_script
        self.token = token
        self.username = username
        self.password = password
        self.headers = None

        # Methods call
        self._auth()

    def _auth(self):
        """
        Reddit Authentication

        :return: No return.
        """
        ### Getting Access
        logger.info('Reddit Authentication started')

        # Request a temporary (2h) OAuth token from Reddit
        # note that CLIENT_ID refers to 'personal use script' and SECRET_TOKEN to 'token'
        auth = requests.auth.HTTPBasicAuth(self.personal_use_script, self.token)

        # Here we pass our login method (password), username, and password
        access_data = {'grant_type': 'password',
                       'username': self.username,
                       'password': self.password}

        # Setup our header info, which gives reddit a brief description of our app
        headers = {'User-Agent': 'watchman/0.0.1'}

        # Send our request for an OAuth token
        res = requests.post('https://www.reddit.com/api/v1/access_token',
                            auth=auth, data=access_data, headers=headers)

        if res.status_code == 200:
            try:
                # convert response to JSON and pull access_token value
                token = res.json()['access_token']

                # add authorization to our headers dictionary
                headers = {** headers, ** {'Authorization': f"bearer {token}"}}

                # while the token is valid (~2 hours) we just add headers=headers to our requests
                requests.get('https://oauth.reddit.com/api/v1/me', headers=headers)

                self.headers = headers
                logger.info('Reddit Authentication ended')

            except KeyError:
                raise Exception("Error: {error}".format(error=res.json()['error']))

        else:
            raise Exception(res.status_code, res.text)

    def get_new_posts(self, communities: list, how_many_posts=1000, write_df_to_bq=False, bq_cred_path=None, bq_destination_table_id=None):
        """
        Get newest posts from Reddit communities, and optionally sent to Google BigQuery.

        :param communities: (list) the list of communities to take posts from.
        :param how_many_posts: (int=1000) how many posts to get.
        :param write_df_to_bq: (bool=False) if the result should be written/appended in a Google BigQuery table.
        :param bq_cred_path: (str=None) Google BigQuery credentials complete path.
        :param bq_destination_table_id: (str=None) Google BigQuery destination table id.
        :return: A pandas Dataframe containing all the posts.
        """
        logger.info(f'Get first {how_many_posts} new posts from: {communities} started')

        # initialize empty dataframe to store posts
        posts = pd.DataFrame()

        # We are going to retrieve the 1000 hottest posts in each community
        for community in communities:

            logger.info('Working on {}'.format(community))
            # loop through 10 times (returning 1000 posts)
            logger.info('{community} - Loop in range: {range}'.format(community=community, range=range(int(how_many_posts / 100))))

            for i in range(int(how_many_posts / 100)):
                # make request
                if i == 0:
                    params = {'limit': 100}
                else:
                    if len(res_result) > 0:
                        last_post = res_result.sort_values(by='created_utc', ascending=True).iloc[0]
                        logger.info("Bucket last post timestamp: {last_post_timestamp}".format(last_post_timestamp=last_post['created_utc']))
                        last_post_fullname = last_post['kind'] + '_' + last_post['id']

                        params = {'limit': 100, 'after': last_post_fullname}
                    else:
                        logger.info("{community} finished!".format(community=community))
                        break

                res = requests.get('https://oauth.reddit.com/r/{}/new'.format(community),
                                   headers=self.headers,
                                   params=params)

                res_result = RedditWatcher._df_from_response(res)
                posts = posts.append(res_result)

        # Set index
        posts.set_index('id', inplace=True)

        # Write to Google BigQuery
        if write_df_to_bq:
            logger.info(f"Start to write the result (nrows: {posts.shape[0]} - ncols: {posts.shape[1]}) to Google BigQuery table {bq_destination_table_id}...")
            job_status = self._write_df_to_bigquery(df=posts, bq_cred_path=bq_cred_path, bq_destination_table_id=bq_destination_table_id)
            if job_status != 'DONE':
                raise Exception(f"Error: Google BigQuery Job status: {job_status}")
            else:
                logger.info("Result successfully written to Google BigQuery.")

        logger.info(f'Get first {how_many_posts} new posts from: {communities} ended')

        return posts

    def get_hot_posts(self, communities, write_df_to_bq=False, bq_cred_path=None, bq_destination_table_id=None):
        """
        Get hottest posts from Reddit communities, and optionally sent to Google BigQuery.

        :param communities: (list) the list of communities to take posts from.
        :param write_df_to_bq: (bool=False) if the result should be written/appended in a Google BigQuery table.
        :param bq_cred_path: (str=None) Google BigQuery credentials complete path.
        :param bq_destination_table_id: (str=None) Google BigQuery destination table id.
        :return: A pandas Dataframe containing all the posts.
        """
        logger.info(f'Get first 100 hot posts from: {communities} started')
        # initialize empty dataframe to store posts
        posts = pd.DataFrame()

        # We are going to retrieve the 100 hottest posts in each community
        for community in communities:
            logger.info('Working on {}'.format(community))

            # make request
            params = {'limit': 100}

            res = requests.get('https://oauth.reddit.com/r/{}/hot'.format(community),
                               headers=self.headers,
                               params=params)

            res_result = RedditWatcher._df_from_response(res)
            posts = posts.append(res_result)
            logger.info("{community} finished!".format(community=community))

        # Set index
        posts.set_index('id', inplace=True)

        # Write to Google BigQuery
        if write_df_to_bq:
            logger.info(f"Start to write the result (nrows: {posts.shape[0]} - ncols: {posts.shape[1]}) to Google BigQuery table {bq_destination_table_id}...")
            job_status = self._write_df_to_bigquery(df=posts, bq_cred_path=bq_cred_path, bq_destination_table_id=bq_destination_table_id)
            if job_status != 'DONE':
                raise Exception(f"Error: Google BigQuery Job status: {job_status}")
            else:
                logger.info("Result successfully written to Google BigQuery.")

        logger.info(f'Get first 100 hot posts from: {communities} ended')

        return posts

    @staticmethod
    def _df_from_response(res: requests.Response):
        """
        Static method to convert http response in pandas Dataframe.

        :param res: The response to convert in pandas DataFrame
        :return: Response converted as pandas DataFrame
        """

        # initialize temp dataframe for batch of data in response
        df = pd.DataFrame()

        # loop through each post pulled from res and append to df
        for post in res.json()['data']['children']:
            df = df.append({
                'subreddit': post['data']['subreddit'],
                'title': post['data']['title'],
                'selftext': post['data']['selftext'],
                'upvote_ratio': post['data']['upvote_ratio'],
                'ups': post['data']['ups'],
                'downs': post['data']['downs'],
                'score': post['data']['score'],
                'total_awards_received': post['data']['total_awards_received'],
                'link_flair_css_class': post['data']['link_flair_css_class'],
                'created_utc': datetime.fromtimestamp(post['data']['created_utc']).strftime('%Y-%m-%dT%H:%M:%SZ'),
                'created': post['data']['created'],
                'id': post['data']['id'],
                'kind': post['kind']
            }, ignore_index=True)

        return df

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
            client = bigquery.Client()

        # Define destinantion table id
        table_id = bq_destination_table_id

        job_config = bigquery.LoadJobConfig(
            # Specify a (partial) schema. All columns are always written to the table.
            # The schema is used to assist in data type definitions for column whose type cannot be auto-detected.
            schema=[
                bigquery.SchemaField("id", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("created", bigquery.enums.SqlTypeNames.FLOAT),
                bigquery.SchemaField("created_utc", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("downs", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("kind", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("link_flair_css_class", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("score", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("selftext", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("subreddit", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("title", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("total_awards_received", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("ups", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("upvote_ratio", bigquery.enums.SqlTypeNames.FLOAT),
            ],
            # Optionally, set the write disposition. BigQuery appends loaded rows
            # to an existing table by default, but with WRITE_TRUNCATE write
            # disposition it replaces the table with the loaded data.
            write_disposition="WRITE_APPEND",
        )

        # Make an API request.
        job = client.load_table_from_dataframe(dataframe=df, destination=table_id, job_config=job_config)

        return job.result().state
