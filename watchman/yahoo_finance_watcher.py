import requests
import json
import logging
import pandas as pd
from datetime import date
from string import Template
from google.cloud import bigquery


class YahooFinanceWatcher:

    def __init__(
            self,
            yahoo_finance_api_key
    ):
        self.yahoo_finance_api_key = yahoo_finance_api_key

    def _retrieve_trending(
            self,
            trending_url: str,
            trending_regions: list
    ):

        # Define DataFrame to store results
        df = pd.DataFrame(columns=['day', 'ticker', 'region'])

        # Define url and header
        url = trending_url + '/{region}'
        headers = {'x-api-key': self.yahoo_finance_api_key}

        # Loop over all regions available
        for region in trending_regions:

            log_message = Template("Get trending from region: $region")
            logging.info(log_message.safe_substitute(region=region))

            try:
                # Make https request
                response = requests.request("GET", url.format(region=region), headers=headers)

                # Decode response in a dict
                response_dict = json.loads(response.text)

                # If any result
                if len(response_dict['finance']['result']) > 0:
                    # Store region results in a dataframe
                    region_df = pd.DataFrame(response_dict['finance']['result'][0]['quotes'])

                    # Rename 'symbol' column, add 'day' column, add 'region' column
                    region_df = region_df.rename(columns={'symbol': 'ticker'})
                    region_df['day'] = date.today()
                    region_df['region'] = region

                    # Append region results to the complete dataframe
                    df = df.append(region_df)

            except Exception as error:
                log_message = Template("Trending from region $region got error: $error")
                logging.error(log_message.safe_substitute(region=region, error=error))

        return df

    def _retrieve_write_close_price(
            self,
            n_round: int,
            n_daily_round: int,
            yf_url: str,
            most_discussed_stocks_query: str,
            ticker_variants_query: str,
            daily_requests: int,
            symbols_per_request: int,
            max_retry: int,
            yf_interval: str,
            yf_range: str,
            write_to_bq=False,
            bq_cred_path=None,
            bq_close_price_delta_id=None,
            bq_close_price_id=None,
            bq_ticker_not_found_id=None
    ):

        # Define yahoo finance header
        headers = {'x-api-key': self.yahoo_finance_api_key}

        # Construct a BigQuery client object.
        if bq_cred_path:
            client = bigquery.Client.from_service_account_json(bq_cred_path)
        else:
            # Logged with the service account which invoke App Engine
            client = bigquery.Client()

        # Retrieve most discussed stocks and transform to a str
        most_discussed_stocks_df = client.query(most_discussed_stocks_query.format(
            close_price_total_tickers=daily_requests * symbols_per_request,
            n_round=n_round,
            n_daily_round=n_daily_round
        )).to_dataframe()
        most_discussed_tickers_str = ', '.join(f"'{stock}'" for stock in most_discussed_stocks_df['ticker'])

        # Check if some ticker is called in a different way
        ticker_variants_df = client.query(ticker_variants_query.format(most_discussed_tickers_str=most_discussed_tickers_str)).to_dataframe()
        most_discussed_stocks_df = pd.merge(left=most_discussed_stocks_df, right=ticker_variants_df, left_on='ticker', right_on='variant', how='left', suffixes=('', '_to_subst'))
        most_discussed_stocks_df['to_subst'] = most_discussed_stocks_df['ticker_to_subst'].notnull()
        most_discussed_stocks_df['ticker'] = most_discussed_stocks_df.apply(lambda x: x['ticker_to_subst'] if x['to_subst'] else x['ticker'], axis=1)

        # Splitting in lists of 10 elements (max yahoo finance API tickers per request)
        requests_list = [most_discussed_stocks_df['ticker'][i:i + 10] for i in range(0, len(most_discussed_stocks_df['ticker']), 10)]

        # Loop over 10-tickers buckets
        for request in requests_list:

            log_message = Template("Get close prices for: $tickers")
            logging.info(log_message.safe_substitute(tickers=request.values))

            # Define DataFrame to store results
            df = pd.DataFrame(columns=['symbol', 'timestamp', 'end', 'start', 'close', 'previousClose', 'chartPreviousClose', 'dataGranularity'])

            # Define query string
            querystring = {"interval": yf_interval, "range": yf_range, "symbols": ','.join(request)}

            # Manage retry
            yahoo_finance_error = True
            retry_count = 0
            while yahoo_finance_error and retry_count < max_retry:
                try:
                    # Make yahoo finance request
                    response = requests.request("GET", yf_url, headers=headers, params=querystring)

                    # Extract info for each ticker
                    for ticker in json.loads(response.text).keys():
                        df_ticker = pd.DataFrame(json.loads(response.text)[ticker])
                        df = df.append(df_ticker)

                    # Convert timestamp to date
                    df['timestamp'] = df['timestamp'].apply(lambda x: date.fromtimestamp(x))
                    # Rename columns
                    df.rename(columns={'timestamp': 'day', 'symbol': 'ticker', 'close': 'close_price'}, inplace=True)
                    # Select subset of columns
                    df = df[['day', 'ticker', 'close_price']]

                    yahoo_finance_error = False

                except Exception as error:
                    log_message = Template('$error').substitute(error=error)
                    logging.error(log_message)
                    retry_count = retry_count + 1

            # Ticker not found in yahoo finance, to later manually review
            ticker_not_found = [ticker for ticker in request if ticker not in df['ticker'].unique()]
            df_ticker_not_found = pd.DataFrame(ticker_not_found, columns=['ticker'])
            df_ticker_not_found['day'] = date.today()
            df_ticker_not_found['cause'] = 'yahoo_finance_error' if yahoo_finance_error else 'not_found'

            if write_to_bq:
                # Write close prices to BigQuery
                log_message = Template("Start to write results (nrows: $nrows - ncols: $ncols) to Google BigQuery table $bq_destination_table_id...")
                logging.info(log_message.safe_substitute(
                    nrows=df.shape[0],
                    ncols=df.shape[1],
                    bq_destination_table_id=bq_close_price_delta_id
                ))

                close_price_job_config = bigquery.LoadJobConfig(
                    schema=[
                        bigquery.SchemaField("day", bigquery.enums.SqlTypeNames.DATE),
                        bigquery.SchemaField("ticker", bigquery.enums.SqlTypeNames.STRING),
                        bigquery.SchemaField("close_price", bigquery.enums.SqlTypeNames.FLOAT)
                    ],
                    write_disposition="WRITE_APPEND",
                )
                close_price_job = client.load_table_from_dataframe(dataframe=df, destination=bq_close_price_delta_id, job_config=close_price_job_config, num_retries=5)
                if close_price_job.result().state != 'DONE':
                    raise Exception(f"Error: Google BigQuery close price Job status: {close_price_job.result().state}")
                else:
                    log_message = Template("Close prices successfully written to Google BigQuery.")
                    logging.info(log_message)

                # Write tickers not found to BigQuery
                log_message = Template("Start to write results (nrows: $nrows - ncols: $ncols) to Google BigQuery table $bq_destination_table_id...")
                logging.info(log_message.safe_substitute(
                    nrows=df_ticker_not_found.shape[0],
                    ncols=df_ticker_not_found.shape[1],
                    bq_destination_table_id=bq_ticker_not_found_id
                ))

                ticker_not_found_job_config = bigquery.LoadJobConfig(
                    schema=[
                        bigquery.SchemaField("day", bigquery.enums.SqlTypeNames.DATE),
                        bigquery.SchemaField("ticker", bigquery.enums.SqlTypeNames.STRING),
                        bigquery.SchemaField("cause", bigquery.enums.SqlTypeNames.STRING)
                    ],
                    write_disposition="WRITE_APPEND",
                )
                ticker_not_found_job = client.load_table_from_dataframe(dataframe=df_ticker_not_found, destination=bq_ticker_not_found_id, job_config=ticker_not_found_job_config, num_retries=5)
                if ticker_not_found_job.result().state != 'DONE':
                    raise Exception(f"Error: Google BigQuery tickers not found Job status: {ticker_not_found_job.result().state}")
                else:
                    log_message = Template("Tickers not found successfully written to Google BigQuery.")
                    logging.info(log_message)

        if write_to_bq:
            # Upsert from close price delta table to close price main table
            log_message = Template("Start to upsert records in Google BigQuery table $bq_destination_table_id...")
            logging.info(log_message.safe_substitute(bq_destination_table_id=bq_close_price_id))

            upsert_close_price_statement = """
                MERGE
                  `{bq_close_price_id}` T
                USING
                  `{bq_close_price_delta_id}` s
                ON
                  T.ticker = S.ticker
                  AND T.day = S.day
                  WHEN MATCHED THEN UPDATE SET close_price = s.close_price
                  WHEN NOT MATCHED
                  THEN
                INSERT
                  ( ticker,
                    day,
                    close_price)
                VALUES
                  (ticker, day, close_price)
            """
            client.query(upsert_close_price_statement.format(bq_close_price_id=bq_close_price_id, bq_close_price_delta_id=bq_close_price_delta_id))

            # Truncate close price delta table
            log_message = Template("Start to truncate records in Google BigQuery table $bq_destination_table_id...")
            logging.info(log_message.safe_substitute(bq_destination_table_id=bq_close_price_delta_id))

            truncate_close_price_delta_statement = """TRUNCATE TABLE `{bq_close_price_delta_id}`"""
            client.query(truncate_close_price_delta_statement.format(bq_close_price_delta_id=bq_close_price_delta_id))

    def write_results(
            self,
            n_round: int,
            n_daily_round: int,
            close_price_url: str,
            close_price_most_discussed_stocks_query: str,
            close_price_ticker_variants_query: str,
            close_price_daily_requests: int,
            close_price_symbols_per_request: int,
            close_price_max_retry: int,
            close_price_interval: str,
            close_price_range: str,
            trending_url: str,
            trending_regions: list,
            write_to_bq=False,
            bq_cred_path=None,
            bq_close_price_delta_id=None,
            bq_close_price_id=None,
            bq_ticker_not_found_id=None,
            bq_trending_table_id=None
    ):
        """
        # TODO: DOC
        :param n_round:
        :param n_daily_round:
        :param close_price_url:
        :param close_price_most_discussed_stocks_query:
        :param close_price_ticker_variants_query:
        :param close_price_daily_requests:
        :param close_price_symbols_per_request:
        :param close_price_max_retry:
        :param close_price_interval:
        :param close_price_range:
        :param trending_url:
        :param trending_regions:
        :param write_to_bq:
        :param bq_cred_path:
        :param bq_close_price_delta_id:
        :param bq_close_price_id:
        :param bq_ticker_not_found_id:
        :param bq_trending_table_id:

        :return: No return.
        """

        ### Close Price
        self._retrieve_write_close_price(
            n_round=n_round,
            n_daily_round=n_daily_round,
            yf_url=close_price_url,
            most_discussed_stocks_query=close_price_most_discussed_stocks_query,
            ticker_variants_query=close_price_ticker_variants_query,
            daily_requests=close_price_daily_requests,
            symbols_per_request=close_price_symbols_per_request,
            max_retry=close_price_max_retry,
            yf_interval=close_price_interval,
            yf_range=close_price_range,
            write_to_bq=write_to_bq,
            bq_cred_path=bq_cred_path,
            bq_close_price_delta_id=bq_close_price_delta_id,
            bq_close_price_id=bq_close_price_id,
            bq_ticker_not_found_id=bq_ticker_not_found_id
        )

        ### Trending
        if n_round == 1:
            # Retrieve trending tickers
            trending_df = self._retrieve_trending(
                trending_url=trending_url,
                trending_regions=trending_regions
            )
            # Write trending to BigQuery
            if write_to_bq:
                log_message = Template("Start to write the result (nrows: $nrows - ncols: $ncols) to Google BigQuery table $bq_destination_table_id...")
                logging.info(log_message.safe_substitute(
                    nrows=trending_df.shape[0],
                    ncols=trending_df.shape[1],
                    bq_destination_table_id=bq_trending_table_id
                ))

                # Define job config
                job_config = bigquery.LoadJobConfig(
                    schema=[
                        bigquery.SchemaField("day", bigquery.enums.SqlTypeNames.DATE),
                        bigquery.SchemaField("ticker", bigquery.enums.SqlTypeNames.STRING),
                        bigquery.SchemaField("region", bigquery.enums.SqlTypeNames.STRING)
                    ],
                    write_disposition="WRITE_APPEND",
                )

                # Submit request
                job_status = self._write_df_to_bigquery(
                    df=trending_df,
                    job_config=job_config,
                    bq_destination_table_id=bq_trending_table_id,
                    bq_cred_path=bq_cred_path
                )
                if job_status != 'DONE':
                    raise Exception(f"Error: Google BigQuery Job status: {job_status}")
                else:
                    log_message = Template("Result successfully written to Google BigQuery.")
                    logging.info(log_message)

    @staticmethod
    def _write_df_to_bigquery(
            df: pd.DataFrame,
            job_config: bigquery.LoadJobConfig,
            bq_cred_path: str,
            bq_destination_table_id: str
    ):
        """
        Static method to write a Pandas.DataFrame in a Google BigQuery table.

        :param df: The Pandas.DataFrame to write.
        :param job_config: Google BigQuery job configuration (schema and write disposition).
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

        # Make an API request
        job = client.load_table_from_dataframe(dataframe=df, destination=bq_destination_table_id, job_config=job_config)

        return job.result().state
