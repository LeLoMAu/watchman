from watchman.reddit_watcher import *
from watchman.twitter_watcher import *
from watchman.config import *
from datetime import datetime, timedelta
from string import Template


def watchman(request):
    try:
        current_time = datetime.utcnow()
        log_message = Template('Cloud Function was triggered on $time')
        logging.info(log_message.safe_substitute(time=current_time))

        try:
            ### TWITTER
            # Twitter Watcher init
            twitter_watcher = TwitterWatcher(bearer_token=config_vars['twitter_bearer_token'])
            df_tweets = twitter_watcher.write_results(
                hashtags=config_vars['twitter_hashtags'],
                start_time=(datetime.now() - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                max_results_per_page=100,
                max_results=config_vars['twitter_max_tweets'],
                write_df_to_bq=True,
                # bq_cred_path=config_vars['bq_cred_path'],
                bq_destination_table_id=config_vars['twitter_tweets_table_id']
            )

            ### REDDIT
            # Reddit Watcher init
            reddit_watcher = RedditWatcher(
                personal_use_script=config_vars['reddit_personal_use_script'],
                token=config_vars['reddit_token'],
                username=config_vars['reddit_username'],
                password=config_vars['reddit_password']
            )
            # Get 10000 newest posts from reddit communities
            df_new_posts = reddit_watcher.get_new_posts(
                communities=config_vars['reddit_communities'],
                how_many_posts=config_vars['reddit_max_new_posts'],
                write_df_to_bq=True,
                # bq_cred_path=config_vars['bq_cred_path'],
                bq_destination_table_id=config_vars['reddit_new_posts_table_id']
            )
            # Get 100 hottest posts from reddit communities
            df_hot_posts = reddit_watcher.get_hot_posts(
                communities=config_vars['reddit_communities'],
                write_df_to_bq=True,
                # bq_cred_path=config_vars['bq_cred_path'],
                bq_destination_table_id=config_vars['reddit_hot_posts_table_id']
            )

        except Exception as error:
            log_message = Template('Query failed due to '
                                   '$message.')
            logging.error(log_message.safe_substitute(message=error))

    except Exception as error:
        log_message = Template('$error').substitute(error=error)
        logging.error(log_message)


# if __name__ == '__main__':
#     watchman('local')
