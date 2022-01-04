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

    # Get request parameters
    request_headers = request.headers
    n_round = int(request_headers.get('n_round', 1))

    # Number of total daily round of this Cloud Function
    n_daily_round = 10

    current_time = datetime.utcnow()
    log_message = Template('Cloud Function watchman_yahoo_finance round $n_round of $n_daily_round was triggered on $time')
    logging.info(log_message.safe_substitute(n_round=n_round, n_daily_round=n_daily_round, time=current_time))

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
            n_round=n_round,
            n_daily_round=n_daily_round,
            close_price_url=config_vars['yf_close_price_url'],
            close_price_most_discussed_stocks_query=config_vars['yf_most_discussed_stocks'],
            close_price_ticker_variants_query=config_vars['yf_ticker_variants'],
            close_price_daily_requests=config_vars['yf_close_price_daily_requests'],
            close_price_symbols_per_request=config_vars['yf_close_price_symbols_per_request'],
            close_price_max_retry=config_vars['yf_close_price_max_retry'],
            close_price_interval=config_vars['yf_close_price_interval'],
            close_price_range=config_vars['yf_close_price_range'],
            trending_url=config_vars['yf_trending_url'],
            trending_regions=config_vars['yf_trending_regions'],
            write_to_bq=True,
            bq_close_price_delta_id=config_vars['yf_close_price_delta'],
            bq_close_price_id=config_vars['yf_close_price'],
            bq_ticker_not_found_id=config_vars['yf_ticker_not_found'],
            bq_trending_table_id=config_vars['yf_trending'],
        )

    except Exception as error:
        log_message = Template('Query failed due to '
                               '$message.')
        logging.error(log_message.safe_substitute(message=error))


# if __name__ == '__main__':
#     watchman_yahoo_finance('local')
