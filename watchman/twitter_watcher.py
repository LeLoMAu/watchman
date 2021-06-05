import logging
import requests
import pandas as pd

logger = logging.getLogger(__name__)


class TwitterWatcher:
    def __init__(self, bearer_token):
        self.bearer_token = bearer_token
        self.headers = {"Authorization": "Bearer {}".format(self.bearer_token)}
        self.url = None

    def make_query(self, hashtags):
        """
        Make a hhtp query from a list of hashtags

        :param hashtags: (list) the list of hashtags.
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
        self.url = "https://api.twitter.com/2/tweets/search/recent?query={query}&{tweet_fields}&{user_expansion}&{user_fields}".format(
            query=query, tweet_fields=tweet_fields, user_expansion=user_expansion, user_fields=user_fields)

        logger.info("Query made.")

    def get_results(self):
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
            # Return the response as json
            logger.info("Url submitted and results obtained.")
            return self._df_from_response(response)
        else:
            logger.warning('Please make a query before submitting.')

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
