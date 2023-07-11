import os
import json
import pandas as pd
from typing import Callable

from battetl import logger, Utils
from battetl.extract import Extractor
from battetl.transform import Transformer
from battetl.load import Loader


class BattETL:
    def __init__(
            self, config_path: str,
            user_transform_test_data: Callable[[
                pd.DataFrame], pd.DataFrame] = None,
            user_transform_cycle_stats: Callable[[
                pd.DataFrame], pd.DataFrame] = None,
            env_path: str = os.path.join(os.getcwd(), '.env')):
        """
        Creates the a class instance. The `start()` method still needs to be run to create the connection
        and to check the validity of the config.

        Parameters
        ----------
        config_path : str
            Path to the json configuration file for the class instance. The configuration file must contain
            the following:
            - `test_name` - Test name should be define in the `test_data_meta` table.
            - `data_file_path` - Absolute or relative path to the test data file.
            - `stats_file_path` - Absolute or relative path to the cycle stats data file.
            - `schedule_file_path` - Absolute or relative path to the schedule or procedure file.
            - `meta_data` - Dictionary containing the meta data for the test.

        user_transform_test_data : Callable[[pd.DataFrame], pd.DataFrame], optional
            A user defined function to transform test data. The function should take a pandas.DataFrame
            as input and return a pandas.DataFrame as output.
        user_transform_cycle_stats : Callable[[pd.DataFrame], pd.DataFrame], optional
            A user defined function to transform cycle stats. The function should take a pandas.DataFrame
            as input and return a pandas.DataFrame as output.
        env_path : str, optional
            Path to the .env file containing the environment variables. The default is the current working
            directory.
        """
        self.env_path = env_path
        Utils.load_env(env_path)

        self.config = Utils.load_config(config_path)

        self.user_transform_test_data = user_transform_test_data
        self.user_transform_cycle_stats = user_transform_cycle_stats

        self.raw_test_data = pd.DataFrame()
        self.test_data = pd.DataFrame()
        self.raw_cycle_stats = pd.DataFrame()
        self.cycle_stats = pd.DataFrame()
        self.schedule = None

    def extract(self):
        """
        Extracts the data in the target directory specified in the config.

        Returns
        -------
        self : BattETL
            Returns a reference to the instance object
        """
        extractor = Extractor()

        # Test data
        if self.config.get('data_file_path'):
            try:
                extractor.data_from_files(self.config['data_file_path'])
                self.raw_test_data = extractor.raw_test_data
            except Exception as e:
                logger.error('Failed to extract test data', exc_info=True)
                logger.error(e)
        else:
            logger.warning('No test data file path')

        # Cycle stats
        if self.config.get('stats_file_path'):
            try:
                extractor.data_from_files(self.config['stats_file_path'])
                self.raw_cycle_stats = extractor.raw_cycle_stats
            except Exception as e:
                logger.error('Failed to extract cycle stats', exc_info=True)
                logger.error(e)
        else:
            logger.warning('No cycle stats file path')

        # Schedule
        if self.config.get('schedule_file_path'):
            try:
                extractor.schedule_from_files(
                    self.config['schedule_file_path'])
                self.schedule = extractor.schedule
                self.config['meta_data']['schedule_meta']['details'] = json.dumps(
                    extractor.schedule)
            except Exception as e:
                logger.error('Failed to extract schedule', exc_info=True)
                logger.error(e)
        else:
            logger.warning('No schedule file path')

        logger.info('Finished extracting data')

        return self

    def transform(self):
        """
        Transforms the test data from the target directory.

        Returns
        -------
        self : BattETL
            Returns a reference to the instance object
        """
        transformer = Transformer(
            timezone=self.config.get('timezone'),
            user_transform_test_data=self.user_transform_test_data,
            user_transform_cycle_stats=self.user_transform_cycle_stats)

        if not self.raw_test_data.empty:
            try:
                transformer.transform_test_data(self.raw_test_data)
                self.test_data = transformer.test_data
            except Exception as e:
                logger.error('Failed to transform test data', exc_info=True)
                logger.error(e)
        else:
            logger.warning('No test data to transform.')

        if not self.raw_cycle_stats.empty:
            try:
                transformer.transform_cycle_stats(self.raw_cycle_stats)
                self.cycle_stats = transformer.cycle_stats
            except Exception as e:
                logger.error('Failed to transform cycle stats', exc_info=True)
                logger.error(e)
        else:
            logger.warning('No cycle stats to transform.')

        if not self.test_data.empty and self.schedule:
            try:
                cv_voltage_threshold_mv = self.config['meta_data']['schedule_meta'].get(
                    'cv_voltage_threshold_mv')
                cell_thermocouple = self.config.get('cell_thermocouple')

                transformer.calc_cycle_stats(
                    self.schedule['steps'],
                    cv_voltage_threshold_mv=cv_voltage_threshold_mv,
                    cell_thermocouple=cell_thermocouple)
                self.cycle_stats = transformer.cycle_stats
            except Exception as e:
                logger.error('Failed to calculate cycle stats', exc_info=True)
                logger.error(e)
        else:
            logger.warning('Skipping cycle stats calculation.')

        logger.info('Finished transforming data')

        return self

    def load(self):
        """
        Loads the test data, cycle stats, and schedule file to the target databases(s) 
        specified in the config.

        Returns
        -------
        num_rows_inserted : int
            The number of rows inserted into the target_table. 
        """

        loader = Loader(
            config=self.config,
            env_path=self.env_path)

        # Load test data
        if not self.test_data.empty:
            num_rows_inserted_test_data = loader.load_test_data(self.test_data)
            logger.info(
                f'Loaded {num_rows_inserted_test_data} rows of test data to database!')
        else:
            logger.warning('No test data to load.')

        # Load cycle stats
        if not self.cycle_stats.empty:
            num_rows_inserted_cycle_stats = loader.load_cycle_stats(
                self.cycle_stats)
            logger.info(
                f'Loaded {num_rows_inserted_cycle_stats} rows of cycle stats to database')
        else:
            logger.warning('No cycle stats to load.')

        del loader

        logger.info('Finished loading data')

        return self
