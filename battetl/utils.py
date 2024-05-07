import os
import re
import json
import yaml
import dotenv
import logging
import pandas as pd
from collections import OrderedDict
from pydash import get, set_with, unset, merge

from battetl import logger, Constants


class Utils:
    def load_env(env_path: str) -> None:
        """
        Set environment variables from .env file

        Parameters
        ----------
        env_path : str
            Path to .env file
        """
        logger.info(f'Load environment variables from {env_path}')
        dotenv.load_dotenv(env_path, override=True)
        # Set logger level
        if os.getenv('ENV') == 'dev':
            logger.info('Set logger level to DEBUG')
            logger.setLevel(logging.DEBUG)
        elif os.getenv('ENV') in ['prd', 'prod']:
            logger.info('Set logger level to WARNING')
            logger.setLevel(logging.WARNING)
        
    def load_config(config_path: str) -> dict:
        """
        Load config file from path

        Parameters
        ----------
        config_path : str
            Path to config file

        Returns
        -------
        config : dict
            Config file as dictionary
        """
        logger.info(f'Load config file from {config_path}')
        config = None

        if not os.path.exists(config_path):
            raise FileNotFoundError(f'Config file not found at {config_path}')

        try:
            with open(config_path, 'r') as f:
                config = json.load(f)
                logger.info('Loaded config file as json')
                return config
        except json.decoder.JSONDecodeError:
            logger.debug('Config file is not json')

        try:
            with open(config_path, 'r') as f:
                config = yaml.load(f, Loader=yaml.SafeLoader)
                logger.info('Loaded config file as yaml')
                return config
        except yaml.YAMLError:
            logger.debug('Config file is not yaml')

        # If we get here, the config file is not json or yaml
        raise Exception('Config file is not json or yaml')

    def drop_unnamed_columns(df: pd.DataFrame) -> pd.DataFrame:
        """
        The function drops unnamed columns from DataFrame

        Parameters
        ----------
        df : pandas.DataFrame
            DataFrame to drop unnamed columns from.

        Returns
        -------
        df : pandas.DataFrame
            Pandas DataFrame with the unnamed columns dropped.
        """
        logger.info('Drop unnamed columns')
        unnamed_column_list = df.filter(like='Unnamed', axis=1).columns
        df = Utils.drop_columns(df, unnamed_column_list)

        return df

    def drop_columns(df: pd.DataFrame, columns_to_drop: list[str]) -> pd.DataFrame:
        """
        This function drops unnamed columns from the passed DataFrame.

        Parameters
        ----------
        df : pandas.DataFrame
            DataFrame to drop unnamed columns from.
        columns_to_drop : list
            List of strings that give column names to drop

        Returns
        -------
        df : pandas.DataFrame
            Pandas DataFrame with the dropped columns.
        """
        for column in columns_to_drop:
            if column in df.columns:
                logger.info(f'Drop column {column}')
                df = df.drop(columns=column)

        return df

    def drop_empty_rows(df: pd.DataFrame) -> pd.DataFrame:
        """
        The function drops empty rows from DataFrame

        Parameters
        ----------
        df : pandas.DataFrame
            DataFrame to drop empty rows from.

        Returns
        -------
        df : pandas.DataFrame
            Pandas DataFrame with the empty rows dropped.
        """
        all_na_count = (df.isna().all(axis=1)).sum()
        if all_na_count > 0:
            logger.info(f'Drop {all_na_count} empty rows')
            df = df.dropna(how='all')

        return df

    def get_cycle_make(columns: list) -> tuple[str, str]:
        """
        Determine the make and type of cycler.
        Currently supported:
        - Arbin Test Data
        - Arbin Cycle Stats
        - Maccor Test Data
        - Maccor Cycle Stats

        Parameters
        ----------
        data : list[str]
            Original data 

        Returns
        -------
        (str)
            Cycle make
        (str)
            Data type
        """
        columnsSet = Utils.get_lower_strip_set(columns)
        arbinTestDataSet = Utils.get_lower_strip_set(
            Constants.COLUMNS_ARBIN_TEST_DATA_ONLY)
        arbinCycleStatsSet = Utils.get_lower_strip_set(
            Constants.COLUMNS_ARBIN_CYCLE_STATS_ONLY)
        maccorTestDataSet = Utils.get_lower_strip_set(
            Constants.COLUMNS_MACCOR_TEST_DATA_ONLY)
        maccorTestDataSetType2 = Utils.get_lower_strip_set(
            Constants.COLUMNS_MACCOR_TEST_DATA_TYPE2_ONLY)
        maccorTestDataSetCustomer1 = Utils.get_lower_strip_set(
            Constants.COLUMNS_MACCOR_TEST_DATA_CUSTOMER1)
        maccorCycleStatsSet = Utils.get_lower_strip_set(
            Constants.COLUMNS_MACCOR_CYCLE_STATS_ONLY)
        maccorCycleStatsSetCustomer1 = Utils.get_lower_strip_set(
            Constants.COLUMNS_MACCOR_CYCLE_STATS_CUSTOMER1)

        if len(columnsSet & arbinTestDataSet) >= (len(arbinTestDataSet) / 2):
            return Constants.MAKE_ARBIN, Constants.DATA_TYPE_TEST_DATA

        if len(columnsSet & arbinCycleStatsSet) >= (len(arbinCycleStatsSet) / 2):
            return Constants.MAKE_ARBIN, Constants.DATA_TYPE_CYCLE_STATS

        if len(columnsSet & maccorTestDataSet) >= (len(maccorTestDataSet) / 2):
            return Constants.MAKE_MACCOR, Constants.DATA_TYPE_TEST_DATA

        if len(columnsSet & maccorTestDataSetType2) >= (len(maccorTestDataSetType2) / 2):
            return Constants.MAKE_MACCOR, Constants.DATA_TYPE_TEST_DATA

        if len(columnsSet & maccorTestDataSetCustomer1) >= (len(maccorTestDataSetCustomer1) / 2):
            return Constants.MAKE_MACCOR, Constants.DATA_TYPE_TEST_DATA

        if len(columnsSet & maccorCycleStatsSet) >= (len(maccorCycleStatsSet) / 2):
            return Constants.MAKE_MACCOR, Constants.DATA_TYPE_CYCLE_STATS

        if len(columnsSet & maccorCycleStatsSetCustomer1) >= (len(maccorCycleStatsSetCustomer1) / 2):
            return Constants.MAKE_MACCOR, Constants.DATA_TYPE_CYCLE_STATS

        return None, None

    def get_lower_strip_set(data: list[str]) -> set:
        """
        Transform string list to lower case set

        Parameters
        ----------
        data : list[str]
            Original data 

        Returns
        -------
        (set)
            Lower case set data
        """
        return set([d.lower().strip().replace(' ', '').replace('_', '') for d in data])

    def rename_df_columns(df: pd.DataFrame, columnsMapping: dict) -> pd.DataFrame:
        """
        Rename column names to BattETL format

        Parameters
        ----------
        df : pandas.DataFrame
            Original data

        Returns
        -------
        df : pandas.DataFrame
            Renamed data
        """
        logger.info('Rename column names to BattETL format')
        logger.debug(f'Columns before renaming: {df.columns.values}')

        # Normalize mapping data
        mappingTable = {k.lower().strip(): v for k,
                        v in columnsMapping.items()}

        # Normalize DataFrame columns
        df.columns = df.columns.str.lower()
        df.columns = df.columns.str.strip()

        # Rename thermocouple columns
        for col_name in df.columns.values:
            match_arbin = re.search(
                Constants.PREFIX_ARBIN_THERMOCOUPLE+r'(\d)+ \(c\)', col_name)
            match_maccor = re.search(
                Constants.PREFIX_MACCOR_THERMOCOUPLE + r'(\d)', col_name)
            match = (match_arbin or match_maccor)
            if match:
                new_col_name = Constants.TEMPLATE_RENAMED_THERMOCOUPLE.replace(
                    'X', match.group(1))
                df = df.rename(columns={col_name: new_col_name})

        # Rename to BattETL format
        df = df.rename(mappingTable, axis='columns')
        logger.debug(f'Columns after renaming: {df.columns.values}')

        return df

    def convert_to_milli(df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert columns to milli- and rename column name

        Parameters
        ----------
        df : pandas.DataFrame
            Original data 

        Returns
        -------
        df : pandas.DataFrame
            Converted data
        """
        logger.info('Convert data to milli-')
        for column in df.columns:
            if column in Constants.COLUMNS_TO_MILLI:
                logger.debug(f'Converting {column}')
                df[column] = df[column].replace(
                    {',': ''}, regex=True).apply(pd.to_numeric, 1)
                df[column] = df[column] * 1e3
                columnName = Constants.COLUMNS_TO_MILLI[column]
                logger.debug(f'Rename column name to {columnName}')
                df = df.rename({column: columnName}, axis='columns')

        return df

    def sort_dataframe(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
        """
        Sort pandas.DataFrame with input columns

        Parameters
        ----------
        df : pandas.DataFrame
            Original data 
        columns : list[str]
            Sort by columns

        Returns
        -------
        df : pandas.DataFrame
            Sorted data
        """
        logger.info(f'Sort DataFrame by {columns}')

        df = df.sort_values(by=columns)
        df = df.reset_index(drop=True)
        logger.debug(f'Sorted rows: {df.shape[0]}')

        return df

    def convert_timedelta_to_seconds(df: pd.DataFrame, column: str) -> pd.DataFrame:
        """
        Convert time delta to seconds

        Parameters
        ----------
        df : pandas.DataFrame
            Original data 
        column : str
            Column to convert

        Returns
        -------
        df : pandas.DataFrame
            Converted data
        """
        logger.info(f'Convert {column} to seconds')

        if column not in df.columns:
            raise NameError(f'Can not find {column}')

        df[column] = round(pd.to_timedelta(df[column]).dt.total_seconds(), 3)

        return df

    def convert_datetime(df: pd.DataFrame, column: str, timezone: str) -> pd.DataFrame:
        """
        Convert datetime to UTC format with time zone

        Parameters
        ----------
        df : pandas.DataFrame
            The input DataFrame
        column : str
            column to convert
        timezone : str
            Time zone strings in the IANA Time Zone Database

        Returns
        -------
        df : pandas.DataFrame
            Converted data
        """
        logger.info(f'Convert {column} to UTC with timezone {timezone}')

        if column not in df.columns:
            raise NameError(f'Can not find {column}')

        df[column] = Utils.__parse_datetime(df, column)
        df[column] = df[column].dt.tz_localize(timezone)
        df[column] = df[column].dt.tz_convert('UTC')

        return df

    def __parse_datetime(df: pd.DataFrame, column: str) -> pd.DataFrame:
        """
        Parse datetime with default format to speed up `pandas.to_datetime`

        This function speeds up the `pandas.to_datetime` process time by
        set the date format instead of traversing each row of data.
        The function first tries our default formats,
        and if it doesn't work, it falls back to the original method.

        Parameters
        ----------
        df : pandas.DataFrame
            The input DataFrame
        column : str
            column to convert

        Returns
        -------
        data : pandas.DataFrame
            Converted data
        """
        format_list = [
            '\t%m/%d/%Y %H:%M:%S.%f',
            '%m/%d/%Y %H:%M:%S.%f',
            '%m/%d/%Y %I:%M:%S %p',
            '%m/%d/%Y %H:%M:%S',
        ]
        for format in format_list:
            try:
                data = pd.to_datetime(df[column], format=format)
                logger.debug(
                    f'Found datetime format "{format}" for column {column}')
                return data
            except:
                pass

        logger.debug(
            f'Can not find format in {format_list}, use default')
        return pd.to_datetime(df[column])

    def convert_to_float(value):
        '''
        Converts value to float if it is a string.

        Parameters
        ----------
        value : str or float
            The value to convert.

        Returns
        -------
        float
        '''
        if isinstance(value, str):
            return float(value.replace(',', ''))
        return value
    
    def validate_file_meta(file_meta: dict) -> bool:
        """
        Validate file_meta
        
        Parameters
        ----------
        file_meta : dict
            Dictionary containing the meta data for the file.
            For example:
            
            .. code-block:: python
            
                {
                    "voltage_mv" : 
                    {
                        "column_name":"volt",
                        "scaling_factor":1,
                    },
                    "current_ma" : 
                    {
                        "column_name":"curr",
                        "scaling_factor":1,
                    },
                }
        
        Returns
        -------
        bool
            True if the file_meta is valid.
        """
        # Check file_meta contains all required keys
        for key in Constants.UNSTRUCTURED_DATA_REQUIRED_KEYS:
            if key not in file_meta:
                raise ValueError(
                    f'file_meta does not contain required key: {key}')

        return True


class DashOrderedDict(OrderedDict):
    """
    Pulled from BEEP:
    https://github.com/TRI-AMDD/beep/blob/master/beep/utils/__init__.py

    Nested data structure with pydash enabled
    getters and setters.  Nested values can
    be set using dot notation, e. g.

    >>> dod = DashOrderedDict()
    >>> dod.set('key1.key2', 5)
    >>> print(dod['key1']['key2'])
    >>> 5
    """

    def set(self, string, value):
        set_with(self, string, value, lambda x: OrderedDict())

    def get_path(self, string, default=None):
        return get(self, string, default=default)

    def unset(self, string):
        unset(self, string)

    def merge(self, obj):
        merge(self, obj)

    def __str__(self):
        return "{}:\n{}".format(self.__class__.__name__, json.dumps(self, indent=4))

    def __repr__(self):
        return self.__str__()
