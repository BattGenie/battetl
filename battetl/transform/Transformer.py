import os
import re
import numpy as np
import pandas as pd
from typing import Callable

from battetl import logger, Constants, Utils


class Transformer:
    def __init__(
            self,
            timezone: str = None,
            user_transform_test_data: Callable[[
                pd.DataFrame], pd.DataFrame] = None,
            user_transform_cycle_stats: Callable[[
                pd.DataFrame], pd.DataFrame] = None,
            env_path: str = os.path.join(os.getcwd(), '.env')) -> None:
        """
        An interface to transform battery test data to BattETL schema.

        Parameters
        ----------
        timezone : str
            Time zone strings in the IANA Time Zone Database. Used to convert to unix timestamp
            in seconds. Default 'America/Los_Angeles'.
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
        Utils.load_env(env_path)

        # Default 'America/Los_Angeles'.
        self.timezone = timezone if timezone else Constants.DEFAULT_TIME_ZONE
        self.user_transform_test_data = user_transform_test_data
        self.user_transform_cycle_stats = user_transform_cycle_stats
        if self.user_transform_test_data:
            logger.info('User defined transform_test_data function found')
        if self.user_transform_cycle_stats:
            logger.info('User defined transform_cycle_stats function found')

        self.test_data = pd.DataFrame()
        self.cycle_stats = pd.DataFrame()

    def transform_test_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Transforms test data to conform to BattETL naming and data conventions

        Parameters
        ----------
        data : pandas.DataFrame
            The input DataFrame
        schedule_steps : dict
            A dictionary containing lists of charge (key->'chg'), discharge (key->'dsg'), and 
            rest (key->'rst') steps from the schedule used to generate the data. Used to calculate
            cycle level statistics (e.g. CV charge time.)

        Returns
        -------
        df : pandas.DataFrame
            The transformed output DataFrame
        """
        logger.info('Transform test data')

        df = data.copy()
        df = Utils.drop_unnamed_columns(df)

        cycleMake, dataType = Utils.get_cycle_make(df.columns)
        logger.info(f'Cycle make: {cycleMake}. Data type: {dataType}')

        if cycleMake == Constants.MAKE_ARBIN and dataType == Constants.DATA_TYPE_TEST_DATA:
            df = self.__transform_arbin_test_data(df)

        if cycleMake == Constants.MAKE_MACCOR and dataType == Constants.DATA_TYPE_TEST_DATA:
            df = self.__transform_maccor_test_data(df)

        # Apply user defined transformation
        if self.user_transform_test_data:
            df = self.user_transform_test_data(df)

        self.test_data = df
        return df

    def transform_cycle_stats(self, data: pd.DataFrame):
        """
        Transforms cycle stats to conform to BattETL naming and data conventions

        Parameters
        ----------
        data : pandas.DataFrame
            The input DataFrame

        Returns
        -------
        df : pandas.DataFrame
            The transformed output DataFrame
        """
        logger.info('Transform cycle stats')

        df = data.copy()
        df = Utils.drop_unnamed_columns(df)

        cycleMake, dataType = Utils.get_cycle_make(df.columns)
        logger.info(f'cycle make: {cycleMake}, data type: {dataType}')

        if cycleMake == Constants.MAKE_ARBIN and dataType == Constants.DATA_TYPE_CYCLE_STATS:
            df = self.__transform_arbin_cycle_stats(df)

        if cycleMake == Constants.MAKE_MACCOR and dataType == Constants.DATA_TYPE_CYCLE_STATS:
            df = self.__transform_maccor_cycle_stats(df)

        # Apply user defined transformation
        if self.user_transform_cycle_stats:
            df = self.user_transform_cycle_stats(df)

        self.cycle_stats = df
        return df

    def __transform_arbin_test_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transforms Arbin test data to conform to BattETL naming and data conventions
        1. Rename columns
        2. Convert to milli
        3. Convert datetime
        4. Convert data type
        5. Sort data

        Parameters
        ----------
        df : pandas.DataFrame
            The input DataFrame

        Returns
        -------
        df : pandas.DataFrame
            The transformed output DataFrame
        """
        df = Utils.rename_df_columns(
            df, Constants.COLUMNS_MAPPING_ARBIN_TEST_DATA)
        df = Utils.convert_to_milli(df)
        df = self.__convert_datetime_unixtime(df)
        df = self.__convert_data_type(df)
        df = Utils.sort_dataframe(df, ['unixtime_s', 'step'])

        return df

    def __transform_arbin_cycle_stats(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transforms Arbin cycle stats to conform to BattETL naming and data conventions
        1. Rename columns
        2. Convert to milli
        3. Convert data type
        4. Sort data

        Parameters
        ----------
        df : pandas.DataFrame
            The input DataFrame

        Returns
        -------
        df : pandas.DataFrame
            The transformed output DataFrame
        """
        df = Utils.rename_df_columns(
            df, Constants.COLUMNS_MAPPING_ARBIN_CYCLE_STATS)
        df = Utils.convert_to_milli(df)
        df = self.__convert_data_type(df)
        df = Utils.sort_dataframe(df, ['cycle'])

        return df

    def __transform_maccor_test_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transforms Maccor test data to conform to BattETL naming and data conventions
        1. Rename columns
        2. Convert to milli
        3. Convert datetime
        4. Convert data type
        5. Sort data

        Parameters
        ----------
        df : pandas.DataFrame
            The input DataFrame

        Returns
        -------
        df : pandas.DataFrame
            The transformed output DataFrame
        """
        df = Utils.rename_df_columns(
            df, Constants.COLUMNS_MAPPING_MACCOR_TEST_DATA)
        df = Utils.convert_to_milli(df)
        df = self.__convert_datetime_unixtime(df)
        df = self.__convert_data_type(df)

        df = Utils.sort_dataframe(df, ['unixtime_s', 'step'])

        return df

    def __transform_maccor_cycle_stats(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transforms Maccor cycle to conform to BattETL naming and data conventions
        1. Rename columns
        2. Convert to milli
        3. Convert data type
        4. Convert test time
        5. Sort data

        Parameters
        ----------
        df : pandas.DataFrame
            The input DataFrame

        Returns
        -------
        df : pandas.DataFrame
            The transformed output DataFrame
        """
        df = Utils.rename_df_columns(
            df, Constants.COLUMNS_MAPPING_MACCOR_CYCLE_STATS)
        df = Utils.convert_to_milli(df)
        df = self.__convert_data_type(df)

        def timedelta_validation_check(input_string):
            # Check if it is like the format "1d 15:07:52.77"
            regex = re.compile('\d+d \d+:\d+:\d+.\d+\Z', re.I)
            match = regex.match(str(input_string))
            return bool(match)

        if 'test_time_s' in df.columns and len(df) > 0 and timedelta_validation_check(df['test_time_s'][0]):
            df = Utils.convert_timedelta_to_seconds(df, 'test_time_s')

        df = Utils.sort_dataframe(df, ['cycle'])

        return df

    def __convert_datetime_unixtime(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert datetime to UTC format and add unixtime_s column

        Parameters
        ----------
        df : pandas.DataFrame
            The input DataFrame

        Returns
        -------
        df : pandas.DataFrame
            The transformed output DataFrame
        """
        logger.info('Convert datetime and add unixtime_s')

        df = Utils.convert_datetime(df, 'recorded_datetime', self.timezone)

        # Convert to unix timestamp
        df['unixtime_s'] = df['recorded_datetime'].astype(np.int64) // 10 ** 9

        return df

    def __convert_data_type(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert some data types if needed

        Parameters
        ----------
        df : pandas.DataFrame
            The input DataFrame

        Returns
        -------
        df : pandas.DataFrame
            The transformed output DataFrame
        """
        # Change the datatype to save on memory
        if 'cycle' in df.columns:
            logger.info('Convert column `cycle` to uint16')
            df['cycle'] = df['cycle'].astype('uint16')
        if 'stop' in df.columns:
            logger.info('Convert column `step` to uint8')
            df['step'] = df['step'].astype('uint8')

        return df

    def __harmonize_capacity(self, df: pd.DataFrame, steps: dict) -> pd.DataFrame:
        """
        Harmonizes capacity/energy values across cyclers by creating four new columns: `charge_capacity_mah`, 
        `charge_energy_mwh`, `discharge_capacity_mah`, and `discharge_energy_mwh`. These columns only report 
        capacity/energy values during corresponding charge or discharge steps. Otherwise, they will be 
        `NaN`. The original capacity/energy reading is preserved in `cycler_charge_capacity_mah` or 
        `cycler_charge_energy_mwh` column, where `cycler` is replaced with the name of the cycler manufacturer.

        Parameters
        ----------
        df : pd.DataFrame
            A pandas DataFrame containing data for single battery cycle.
        steps : dict
            A dictionary containing lists of charge (key->'chg'), discharge (key->'dsg'), and rest (key->'rst') steps.

        Returns
        -------
        df : pd.DataFrame
            A pandas DataFrame with harmonized capacity/energy values across cyclers.
        """

        if 'maccor_capacity_mah' in df:
            df['charge_capacity_mah'] = df[df.step.isin(
                steps['chg'])].maccor_capacity_mah
            df['discharge_capacity_mah'] = df[df.step.isin(
                steps['dsg'])].maccor_capacity_mah
        if 'maccor_energy_mwh' in df:
            df['charge_energy_mwh'] = df[df.step.isin(
                steps['chg'])].maccor_energy_mwh
            df['discharge_energy_mwh'] = df[df.step.isin(
                steps['dsg'])].maccor_energy_mwh
            # log rows and columns that be modified
            logger.debug(
                f'Harmonized capacity/energy values for Maccor cyclers. Modified {len(df)} rows and 4 columns.')
            logger.debug(
                'Added columns: charge_capacity_mah, charge_energy_mwh, discharge_capacity_mah, discharge_energy_mwh')
        elif 'arbin_charge_capacity_mah' in df:
            df['charge_capacity_mah'] = df[df.step.isin(
                steps['chg'])].arbin_charge_capacity_mah
            df['charge_energy_mwh'] = df[df.step.isin(
                steps['chg'])].arbin_charge_energy_mwh
            df['discharge_capacity_mah'] = df[df.step.isin(
                steps['dsg'])].arbin_discharge_capacity_mah
            df['discharge_energy_mwh'] = df[df.step.isin(
                steps['dsg'])].arbin_discharge_energy_mwh
            logger.debug(
                f'Harmonized capacity/energy values for Arbin cyclers. Modified {len(df)} rows and 4 columns.')
            logger.debug(
                'Added columns: charge_capacity_mah, charge_energy_mwh, discharge_capacity_mah, discharge_energy_mwh')
        else:
            logger.warning("No capacity columns were found to refactor!")

        return df

    def calc_cycle_stats(self, steps: dict, cv_voltage_thresh_mv: float):
        """
        Calculates various charge and discharge statistics at the cycle level. Note this function 
        can only be run after self.test_data exists

        Parameters
        ----------
        steps : dict
            A dictionary containing lists of charge (key->'chg'), discharge (key->'dsg'), and 
            rest (key->'rst') steps.
        cv_voltage_thresh_mv : float
            The the voltage threshold in milli-volts above which charge is considered to be constant voltage.

        Returns
        -------
        stats : dict
            A dictionary containing various charge and discharge stats.
        """
        logger.debug(
            f'Calculating cycle statistics with cv_voltage_thresh_mv: {cv_voltage_thresh_mv}')

        if self.test_data.empty:
            logger.error("Cannot run `calc_cycle_stats()` without test_data!")
            return {}

        self.test_data = self.__harmonize_capacity(self.test_data, steps)

        # DataFrame where we will hold calculated cycle statistics for all cycles
        df_calced_stats = pd.DataFrame(columns=['cycle'])

        for cycle in range(self.test_data.cycle.head(1).item(), self.test_data.cycle.tail(1).item()):

            cycle_data = self.test_data[self.test_data.cycle == cycle]
            if cycle_data.empty:
                logger.info("No cycle data for cycle " + str(cycle))
                continue

            # Calculate various charge and discharge cycle statistics.
            stats = {'cycle': cycle}

            charge_metrics = self.__calc_charge_stats(
                cycle_data, steps['chg'], cv_voltage_thresh_mv)
            stats.update(charge_metrics)

            discharge_metrics = self.__calc_discharge_stats(
                cycle_data, steps['dsg'])
            stats.update(discharge_metrics)

            # Calculate coulombic efficiency from the charge and discharge stats.
            if (('calculated_charge_capacity_mah' not in stats) or
                    ('calculated_discharge_capacity_mah' not in stats) or 
                        (stats['calculated_charge_capacity_mah']==0)):
                ce = float("nan")
                logger.info(
                    "Unable to calculate coulombic efficiency for cycle " + str(cycle))
            else:
                ce = (discharge_metrics['calculated_discharge_capacity_mah']
                      / charge_metrics['calculated_charge_capacity_mah'])
            stats.update({'coulombic_efficiency': ce})

            # Append the cycle statistics from this cycle to our cumulative DataFrame for all cycles.
            df_calced_stats = pd.concat(
                [df_calced_stats, pd.DataFrame(stats, index=[0])], axis=0)

        if self.cycle_stats.empty:
            self.cycle_stats = df_calced_stats
        else:
            self.cycle_stats = self.cycle_stats.join(
                df_calced_stats.set_index('cycle'), on='cycle')

        return self.cycle_stats

    def __calc_charge_stats(self, cycle_data: pd.DataFrame, charge_steps: list, cv_voltage_thresh_mv: float) -> dict:
        """
        Calculates various charge stats for a single cycle of data.

        Parameters
        ----------
        cycle_data : pd.DataFrame
            A dataframe containing data for single battery cycle.
        charge_steps : list
            List of charge steps from the cycler schedule.
        cv_voltage_thresh_mv : float
            The the voltage threshold in milli-volts above which charge is considered to be constant voltage.

        Returns
        -------
        stats : dict
            A dictionary containing various charge stats.
        """
        logger.debug(
            f'Calculating charge statistics with cv_voltage_thresh_mv: {cv_voltage_thresh_mv}')
        stats = {}

        # Define charge data to be where the step is a charge step.
        chg_data = cycle_data[cycle_data.step.isin(charge_steps)]
        if len(chg_data) < 2:
            logger.info("No charge data for cycle " +
                        str(cycle_data.cycle.iloc[0]))
            return stats

        cumulative_tuple = self.__calc_cumulative_capacity(
            chg_data, charge_steps, 'charge')
        stats['calculated_charge_capacity_mah'] = cumulative_tuple[0]
        stats['calculated_charge_energy_mwh'] = cumulative_tuple[1]
        stats['calculated_charge_time_s'] = (chg_data.test_time_s.iloc[-1] -
                                             chg_data.test_time_s.iloc[0])

        # Calculate cc/cv time & capacity.
        chg_data_cv = chg_data[chg_data.voltage_mv > cv_voltage_thresh_mv]
        if chg_data_cv.empty:
            logger.info("No CV data for cycle " +
                        str(cycle_data.cycle.iloc[0]))
            stats['calculated_cc_charge_time_s'] = stats['calculated_charge_time_s']
            stats['calculated_cv_charge_time_s'] = float("nan")
            stats['calculated_cc_capacity_mah'] = stats['calculated_charge_capacity_mah']
            stats['calculated_cv_capacity_mah'] = float("nan")
        else:
            stats['calculated_cc_charge_time_s'] = (chg_data_cv.iloc[0].test_time_s
                                                    - chg_data.iloc[0].test_time_s)
            stats['calculated_cv_charge_time_s'] = (chg_data_cv.test_time_s.iloc[-1]
                                                    - chg_data_cv.test_time_s.iloc[0])
            stats['calculated_cc_capacity_mah'] = chg_data_cv.iloc[0].charge_capacity_mah
            stats['calculated_cv_capacity_mah'] = (chg_data_cv.charge_capacity_mah.iloc[-1]
                                                   - chg_data_cv.charge_capacity_mah.iloc[0])

        # Calculate 50%/80% charge time & capacity.
        eighty_percent_cap_mah = stats['calculated_charge_capacity_mah'] * 0.8
        fifty_percent_cap_mah = stats['calculated_charge_capacity_mah'] * 0.5
        try:
            eighty_percent_cap_time_s = (chg_data[chg_data.charge_capacity_mah > eighty_percent_cap_mah].test_time_s.iloc[0]
                                        - chg_data.test_time_s.iloc[0])
            half_percent_cap_time_s = (chg_data[chg_data.charge_capacity_mah > fifty_percent_cap_mah].test_time_s.iloc[0]
                                    - chg_data.test_time_s.iloc[0])
        except:
            eighty_percent_cap_time_s = float('nan')
            half_percent_cap_time_s = float('nan')
            logger.warning(
                f'Incomplete charge data for cycle {cycle_data.cycle.iloc[0]}')
            
        stats['calculated_fifty_percent_charge_time_s'] = half_percent_cap_time_s
        stats['calculated_eighty_percent_charge_time_s'] = eighty_percent_cap_time_s

        return stats

    def __calc_discharge_stats(self, cycle_data: pd.DataFrame, discharge_steps: list) -> dict:
        """
        Calculates various discharge stats for a single cycle of data.

        Parameters
        ----------
        cycle_data : pd.DataFrame
            A dataframe containing data for single battery cycle.
        discharge_steps : list
            List of discharge steps from the cycler schedule.

        Returns
        -------
        stats : dict
            A dictionary containing various discharge stats.
        """
        logger.debug('Calculating discharge statistics')
        stats = {}

        # Define discharge data to be where the step is a discharge step.
        dsg_data = cycle_data[cycle_data.step.isin(discharge_steps)]
        if len(dsg_data) < 2:
            logger.info("No discharge data for cycle " +
                        str(cycle_data.cycle.iloc[0]))
            return stats

        cumulative_tuple = self.__calc_cumulative_capacity(
            dsg_data, discharge_steps, 'discharge')
        stats['calculated_discharge_capacity_mah'] = cumulative_tuple[0]
        stats['calculated_discharge_energy_mwh'] = cumulative_tuple[1]
        stats['calculated_discharge_time_s'] = dsg_data.test_time_s.iloc[-1] - \
            dsg_data.test_time_s.iloc[0]

        return stats

    def __calc_cumulative_capacity(self, df: pd.DataFrame, steps: list, step_type: str) -> tuple:
        """
        Calculates cumulative capacity from the dataframe from steps within 
        the predefined list.

        Parameters
        ----------
        df : pd.DataFrame
            A dataframe containing data to calculate capacity from. Will be modified
            in place if capacity is reset after each step.
        steps : list
            A list of the steps to calculate cumulative
        step_type : str
            The type of data we are calculating for. Can either be 'charge' or 'discharge'. If 
            something else is passed an error is thrown.

        Returns
        -------
        (cumulative_capacity_mah, cumulative_energy_mwh): tuple
            Tuple containing cumulative capacity and cumulative energy from the step data.
        """
        logger.debug(f'Calculating cumulative {step_type} capacity')

        if step_type == 'charge':
            capacity_column = 'charge_capacity_mah'
            energy_column = 'charge_energy_mwh'
        elif step_type == 'discharge':
            capacity_column = 'discharge_capacity_mah'
            energy_column = 'discharge_energy_mwh'

        logger.debug(
            f'Capacity column: {capacity_column}. Energy column: {energy_column}')

        # Calculate cumulative capacity/energy from each of the steps.
        cumulative_capacity_mah = 0
        cumulative_energy_mwh = 0
        for i, step in enumerate(steps):
            step_data = df[df.step == step]
            if step_data.empty:
                continue

            if i == 0:
                cumulative_capacity_mah = step_data[capacity_column].iloc[-1] - \
                    step_data[capacity_column].iloc[0]
                cumulative_energy_mwh = step_data[energy_column].iloc[-1] - \
                    step_data[energy_column].iloc[0]
            else:
                # This check statement addresses instances where capacity was reset after each step.
                if step_data[capacity_column].iloc[0] < cumulative_capacity_mah:
                    step_data[capacity_column] += cumulative_capacity_mah
                    step_data[energy_column] += cumulative_energy_mwh
                cumulative_capacity_mah += step_data[capacity_column].iloc[-1] - \
                    step_data[capacity_column].iloc[0]
                cumulative_energy_mwh += step_data[energy_column].iloc[-1] - \
                    step_data[energy_column].iloc[0]

        return (cumulative_capacity_mah, cumulative_energy_mwh)
