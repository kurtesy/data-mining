"""
@title: CSV file Merging script for Prodapt Sobers
@author: nishant patel [kurtesy]
@version: Python 2.7.11
@description: script will parse multiple csv's and create a unified csv.
              Files 3 or more
              Required result format: CSV
              Optional result format: JSON, XML or DB
"""
import pandas as pd
import unittest
import itertools

"""Take a dataframe row and converts it to XML format"""
def to_xml(row):
    xml = ['<item>']
    for field in row.index:
        xml.append('  <field name="{0}">{1}</field>'.format(field, row[field]))
    xml.append('</item>')
    return '\n'.join(xml)

"""
    Extract file name from path/url
    :param: url-> path of csv files
    :return: file_name -> str
    """
def get_filename(url):
    parts = url.split('/')
    return parts[-1]


class CsvMerger:
    COLUMN_MAP = {'timestamp': 'date/timestamp',
                  'date': 'date/timestamp',
                  'date_readable': 'date/timestamp',
                  'to': 'to',
                  'from': 'from',
                  'amounts': 'amount',
                  'amount': 'amount',
                  'transaction': 'transaction_type',
                  'type': 'transaction_type'}

    def __init__(self):
        self.csvData = {}
        self.currencies = ['euro', 'cents', 'dollar', 'rupee', 'pound']
        self.amount = pd.Series()
        self.result_file = 'unified_csv.csv'
        self.result = None

    """
    Reads the CSV files passed and store them as dict of DataFrame
    :param: file_urls-> list of path/url to csv files
    :return None
    """

    def read_csv(self, file_urls=[]):
        for url in file_urls:
            fileName = get_filename(url)
            dataFrame = pd.read_csv(url)
            self.csvData[fileName] = dataFrame

    """
    Normalises the column name is different CSVs to a stand naming as per the COLUMN_MAP
    :param: None
    :return None
    """

    def normalise_column_names(self):
        for key in self.csvData.keys():
            currency = []
            dataFrame = self.csvData[key]
            for column in dataFrame.columns:
                if column in self.currencies:
                    currency.append(column)
                    self.handle_currency_column(dataFrame[column], column)
            dataFrame = self.clean_dataframe(dataFrame, currency)
            self.csvData[key] = dataFrame

    """
    Handles case when there are currency columns and need to normalise them to amounts column
    :param: series-> pandas series which have all column values
    :return: None
    """

    def handle_currency_column(self, series, column):
        if column == 'cents':
            series = series.divide(100, fill_value=0)
        if self.amount.empty:
            self.amount = series
        else:
            self.amount += series

    """
    Clean columns, ie rename and remove redundant columns
    :param: dataFrame, currency
    :return: dataFrame
    """

    def clean_dataframe(self, dataFrame, currency):
        dataFrame = dataFrame.rename(self.COLUMN_MAP, axis=1)
        if not self.amount.empty:
            dataFrame['amount'] = self.amount
            self.amount = pd.Series()
            # Removed residual currency column since they are converted to amount column
            dataFrame = dataFrame.drop(currency, axis=1)
        return dataFrame

    """
   Merge all process dataframes
   :param: None
   :return: Status-> True/False and Merged dataFrame/Error
   """

    def merge_csvs(self):
        try:
            result = pd.concat(self.csvData.values(), ignore_index=True, sort=True)
            self.write_result(result)
            self.result = result
            return True, result
        except Exception as error:
            return False, error

    """Writes result to csv file"""
    def write_result(self, result):
        result.to_csv(self.result_file, encoding='utf-8', index=False, header=True)

    """Fetch result as json"""
    def convert_result_to(self, format):
        formatMap = {'json': lambda z: self.result.to_json(),
                     'xml': lambda z: '\n'.join(self.result.apply(to_xml, axis=1)),
                     'db': lambda z: self.result.to_json(orient='table')}
        if self.result and format in formatMap:
            return formatMap[format]
        return 'No Result generated, first execute run() OR the given format is not supported'


    """Main Driver function"""
    def run(self, fileList):
        self.read_csv(fileList)
        self.normalise_column_names()
        status, result = self.merge_csvs()
        print 'CSV Merge was' + ('completed with result in: {}'.format(self.result_file)
                                 if status else 'failed with error: {}'.format(result))


if __name__ == '__main__':
    testObj = CsvMerger()
    bankFileList = [
        'https://gist.github.com/Attumm/3927bfab39b32d401dc0a4ca8db995bd/raw/c0e20856d481d3b97c3e9e76efe8f0211fbd8aaa/bank1.csv',
        'https://gist.github.com/Attumm/3927bfab39b32d401dc0a4ca8db995bd/raw/c0e20856d481d3b97c3e9e76efe8f0211fbd8aaa/bank2.csv',
        'https://gist.github.com/Attumm/3927bfab39b32d401dc0a4ca8db995bd/raw/c0e20856d481d3b97c3e9e76efe8f0211fbd8aaa/bank3.csv']
    testObj.run(bankFileList)


class TestCase(unittest.TestCase):
    def setUp(self):
        self.testObj = CsvMerger()
        self.TestList = [
            'https://gist.github.com/Attumm/3927bfab39b32d401dc0a4ca8db995bd/raw/c0e20856d481d3b97c3e9e76efe8f0211fbd8aaa/bank1.csv',
            'https://gist.github.com/Attumm/3927bfab39b32d401dc0a4ca8db995bd/raw/c0e20856d481d3b97c3e9e76efe8f0211fbd8aaa/bank2.csv']

    def test_read_csv(self):
        self.testObj.read_csv(self.TestList)
        assert len(self.testObj.csvData.keys()) == 2

    def test_normalise_column_names(self):
        self.testObj.read_csv(self.TestList)
        self.testObj.normalise_column_names()
        verify_columns = set('date/timestamp', 'transaction_type', 'amount', 'from', 'to')
        actual_columns = [dataFrame for dataFrame in self.csvData.values()]
        actual_columns = set(itertools.chain(*actual_columns))
        assert len(verify_columns.difference(actual_columns)) == 0

    def test_merge_csvs_success(self):
        self.testObj.read_csv(self.TestList)
        self.testObj.normalise_column_names()
        status, result = self.testObj.merge_csvs()
        assert status

    def test_merge_csvs_failure(self):
        self.testObj.read_csv(self.TestList)
        self.testObj.normalise_column_names()
        self.testObj.csvData = {}
        status, result = self.testObj.merge_csvs()
        assert not status
