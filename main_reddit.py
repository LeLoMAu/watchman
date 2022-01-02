from watchman.reddit_watcher import *
from watchman.twitter_watcher import *
from watchman.config import *
from datetime import datetime
from string import Template
from google.cloud import secretmanager


def watchman_reddit(request):
    """
    Cloud Function to download reddit posts and store them in BigQuery.

    :param request: a placeholder fot Cloud Function purpose.
    :return: No return.
    """
    current_time = datetime.utcnow()
    log_message = Template('Cloud Function was triggered on $time')
    logging.info(log_message.safe_substitute(time=current_time))

    try:
        # Get Reddit credentials
        log_message = Template('Accessing Reddit credential secrets.')
        logging.info(log_message)

        # Init Secret Manager Service Client
        client = secretmanager.SecretManagerServiceClient()

        # Access secrets
        reddit_personal_use_script = client.access_secret_version(request={"name": "projects/141025174742/secrets/reddit_personal_use_script/versions/1"}).payload.data.decode("utf-8")
        reddit_token = client.access_secret_version(request={"name": "projects/141025174742/secrets/reddit_token/versions/1"}).payload.data.decode("utf-8")
        reddit_username = client.access_secret_version(request={"name": "projects/141025174742/secrets/reddit_username/versions/1"}).payload.data.decode("utf-8")
        reddit_password = client.access_secret_version(request={"name": "projects/141025174742/secrets/reddit_password/versions/1"}).payload.data.decode("utf-8")

    except Exception as error:
        log_message = Template('$error').substitute(error=error)
        logging.error(log_message)

    try:
        # Reddit Watcher init
        reddit_watcher = RedditWatcher(
            personal_use_script=reddit_personal_use_script,
            token=reddit_token,
            username=reddit_username,
            password=reddit_password
        )

        # Get 10000 newest posts from reddit communities
        reddit_watcher.get_new_posts(
            communities=config_vars['reddit_communities'],
            how_many_posts=config_vars['reddit_max_new_posts'],
            write_df_to_bq=True,
            bq_destination_table_id=config_vars['reddit_new_posts']
        )

        # Get 100 hottest posts from reddit communities
        reddit_watcher.get_hot_posts(
            communities=config_vars['reddit_communities'],
            write_df_to_bq=True,
            bq_destination_table_id=config_vars['reddit_hot_posts']
        )

    except Exception as error:
        log_message = Template('Query failed due to '
                               '$message.')
        logging.error(log_message.safe_substitute(message=error))


# if __name__ == '__main__':
#     watchman_reddit('local')
