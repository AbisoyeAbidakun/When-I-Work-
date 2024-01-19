import argparse
import pandas as pd
import string
import validators
import logging

logging.basicConfig(level=logging.INFO)

class DataProcessor:

    """
    A class that takes in a url as input downloads the web traffic data in csv from the url,
    transforms the web traffic data stored in time-record format where
    each row is a page view into a per-user format where each row is a different user and the
    columns represent the time spent on each of the pages.
    : user_id
    : path
    : length
    To make  a new dataframe ofr

    Parameters:
    - url (str): The URL to be validated.
    - column: column used to pivot
    - index: index used as rows on the new pivoted table
    - value: value of each records
    - destination_file_name: the destination of the generated csv file,

    Returns:
    - output: A saved csv file with the process data according to the requirement

   """


    def __init__(
            self,
            url: str,
            column: str,
            index: str,
            value: str,
            destination_file_name: str,
            destination_file_path: str,
    ) -> None:
        self.endpoint =  self._format_endpoint(url)
        self._lever = column
        self._index = index
        self._value = value
        self.destination_file_name = destination_file_name
        self.destination_file_path = destination_file_path

    def _download_file(self, full_path) -> pd.DataFrame:
        """
    	This function:
        - downloads the data from the url,
        - drops of unwanted dolumns
        -  pivots the data using the respective columns

   		Parameters:
   		- full_path (str): The full url path.

        Returns:
        - df: A pivoted dataframe
        """

        df = pd.read_csv(full_path)
        #df = df.drop(['drop', 'user_agent'], axis=1)
        df = df.drop(df.columns.difference([self._index,self._lever,self._value]), axis=1)
        df = df.pivot_table(index=self._index, columns=self._lever, values=self._value).fillna(0)
        df.reset_index(inplace=True)
        return df

    def process_data(self) -> pd.DataFrame:
        """
    	This function:
        - validated the url endpoint,
        - process and combines the individual pivoted dataframes into o

        Returns:
        - final_df: A pivoted and combined dataframe
        """
        source_files = list(string.ascii_lowercase)
        final_df = pd.DataFrame()

        for files in source_files:
            if validators.url(f'{self.endpoint}{files}.csv'):
                full_path = f'{self.endpoint}{files}.csv'

                logging.info("Download file: %s.csv from %s ", files, self.endpoint)

                insert_df = self._download_file(full_path)
                final_df = pd.concat([final_df, insert_df]).reset_index(drop=True)
            else:
                logging.info("Invalid URL Path  %s%s.csv ",
                    self.endpoint,
                    files)
                print(f'url{self.url} is invalid')
        logging.info ("Data processing was successful")
        return final_df

    @staticmethod
    def _format_endpoint(endpoint: str) -> str:
        if endpoint is not None:
            return endpoint if endpoint.endswith("/") else f'{endpoint}/'
        return ""

def main():

    parser = argparse.ArgumentParser(description='Process web traffic data.')
    parser.add_argument('--url', type=str, required=True, help='URL endpoint for data files')
    parser.add_argument('--column', type=str, required=True, help='Column to implement pivoting')
    parser.add_argument('--index', type=str, required=True, help='Index of the  pivoted table')
    parser.add_argument('--value', type=str, required=True, help='Value of the pivoted table')
    parser.add_argument('--destination_file_name', type=str, required=True, help='Output file destination')
    parser.add_argument('--destination_file_path', type=str, required=True, help='Location of file path')

    args = parser.parse_args()

    web_traffic_etl = DataProcessor(
        url=args.url,
        column=args.column,
        index=args.index,
        value=args.value,
        destination_file_name=args.destination_file_name,
        destination_file_path=args.destination_file_path
    )

    final_dataset = web_traffic_etl.process_data()
    final_dataset.to_csv(f'{args.destination_file_path}{args.destination_file_name}.csv',index=False)

if __name__ == "__main__":
    main()

