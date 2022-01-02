from watchman.twitter_watcher import *
from watchman.config import *
from datetime import datetime, timedelta
from string import Template
from google.cloud import secretmanager


def watchman_twitter(request):
    """
    Cloud Function to download tweets and store them in BigQuery.

    :param request: a placeholder fot Cloud Function purpose.
    :return: No return.
    """
    current_time = datetime.utcnow()
    log_message = Template('Cloud Function watchman_twitter was triggered on $time')
    logging.info(log_message.safe_substitute(time=current_time))

    try:
        # Get Twitter bearer token
        log_message = Template('Accessing Twitter bearer token secret.')
        logging.info(log_message)

        # Init Secret Manager Service Client
        client = secretmanager.SecretManagerServiceClient()

        # Access secret
        twitter_bearer_token_secret = client.access_secret_version(request={"name": "projects/141025174742/secrets/twitter_bearer_token/versions/1"})
        twitter_bearer_token_value = twitter_bearer_token_secret.payload.data.decode("utf-8")

    except Exception as error:
        log_message = Template('$error').substitute(error=error)
        logging.error(log_message)

    try:
        # Twitter Watcher init
        twitter_watcher = TwitterWatcher(bearer_token=twitter_bearer_token_value)
        twitter_watcher.write_results(
            hashtags=config_vars['twitter_hashtags'],
            start_time=(datetime.now() - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            max_results_per_page=100,
            max_results=config_vars['twitter_max_tweets'],
            write_df_to_bq=True,
            bq_destination_table_id=config_vars['twitter_tweets']
        )

    except Exception as error:
        log_message = Template('Query failed due to '
                               '$message.')
        logging.error(log_message.safe_substitute(message=error))


# if __name__ == '__main__':
#     watchman_twitter('local')
