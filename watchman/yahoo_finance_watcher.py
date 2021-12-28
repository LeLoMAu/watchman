import requests
import json
import logging
import pandas as pd
from datetime import date
from string import Template
from google.cloud import bigquery


class YahooFinanceWatcher:

    def __init__(self, yahoo_finance_api_key):
        self.yahoo_finance_api_key = yahoo_finance_api_key

    def _retrieve_trending(self, trending_url: str, trending_regions: list):

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

    def write_results(self, trending_url: str, trending_regions: list, write_df_to_bq=False, bq_cred_path=None, bq_trending_table_id=None):
        """
        # TODO: DOC
        :param trending_url:
        :param trending_regions:
        :param write_df_to_bq:
        :param bq_cred_path:
        :param bq_trending_table_id:
        :return:
        """

        ### TRENDING
        # Retrieve trending tickers
        trending_df = self._retrieve_trending(trending_url=trending_url, trending_regions=trending_regions)
        # Write trending to BigQuery
        if write_df_to_bq:
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
            job_status = self._write_df_to_bigquery(df=trending_df, job_config=job_config, bq_destination_table_id=bq_trending_table_id, bq_cred_path=bq_cred_path)
            if job_status != 'DONE':
                raise Exception(f"Error: Google BigQuery Job status: {job_status}")
            else:
                log_message = Template("Result successfully written to Google BigQuery.")
                logging.info(log_message)

    @staticmethod
    def _write_df_to_bigquery(df: pd.DataFrame, job_config: bigquery.LoadJobConfig, bq_cred_path: str, bq_destination_table_id: str):
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
