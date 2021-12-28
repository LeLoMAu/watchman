from watchman.yahoo_finance_watcher import *
from watchman.config import *
from datetime import datetime
from string import Template
from google.cloud import secretmanager


def watchman_yahoo_finance(request):
    """
    Cloud Function to download tweets and store them in BigQuery.

    :param request: a placeholder fot Cloud Function purpose.
    :return: No return.
    """
    current_time = datetime.utcnow()
    log_message = Template('Cloud Function watchman_yahoo_finance was triggered on $time')
    logging.info(log_message.safe_substitute(time=current_time))

    try:
        # Get Yahoo Finance API key
        log_message = Template('Accessing Yahoo Finance API key.')
        logging.info(log_message)

        # Init Secret Manager Service Client
        client = secretmanager.SecretManagerServiceClient()

        # Access secret
        yahoo_finance_api_key_secret = client.access_secret_version(
            request={"name": "projects/141025174742/secrets/yahoo_finance_api_key/versions/1"})
        yahoo_finance_api_key_value = yahoo_finance_api_key_secret.payload.data.decode("utf-8")

    except Exception as error:
        log_message = Template('$error').substitute(error=error)
        logging.error(log_message)

    try:
        # Yahoo Finance Watcher init
        yahoo_finance_watcher = YahooFinanceWatcher(yahoo_finance_api_key=yahoo_finance_api_key_value)
        yahoo_finance_watcher.write_results(
            trending_url=config_vars['yahoo_finance_trending_url'],
            trending_regions=config_vars['yahoo_finance_trending_regions'],
            bq_trending_table_id=config_vars['yahoo_finance_trending_table_id'],
            write_df_to_bq=True
        )

    except Exception as error:
        log_message = Template('Query failed due to '
                               '$message.')
        logging.error(log_message.safe_substitute(message=error))


# if __name__ == '__main__':
#     watchman_yahoo_finance('local')
