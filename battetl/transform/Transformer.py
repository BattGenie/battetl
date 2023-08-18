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
                pd.DataFrame], pd.DataFrame] = None) -> None:
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
        """
        # Default 'America/Los_Angeles'.
        self.timezone = timezone if timezone else Constants.DEFAULT_TIME_ZONE
        self.user_transform_test_data = user_transform_test_data
        self.user_transform_cycle_stats = user_transform_cycle_stats
        if self.user_transform_test_data:
            logger.info('User defined transform_test_data function found')
        if self.user_transform_cycle_stats:
            logger.info('User defined transform_cycle_stats function found')

        self.test_data = pd.DataFrame(dtype=object)
        self.cycle_stats = pd.DataFrame(dtype=object)

    def transform_test_data(self, data: pd.DataFrame, file_meta: dict = None) -> pd.DataFrame:
        """
        Transforms test data to conform to BattETL naming and data conventions

        Parameters
        ----------
        data : pandas.DataFrame
            The input DataFrame
        schedule_steps : dict
            A dictionary containing lists of charge (key->'chg'), discharge (key->'dsg'), and 
            rest (key->'rst') steps from the schedule used to generate the data. Used to calculate
            cycle level statistics (e.g. CV charge time.)s
        file_meta : dict, optional
            Dictionary containing the user defined column names for the test data. The default is None.
        Returns
        -------
        df : pandas.DataFrame
            The transformed output DataFrame
        """
        logger.info('Transform test data')

        df = data.copy()
        df = Utils.drop_unnamed_columns(df)
        df = Utils.drop_empty_rows(df)

        cycleMake, dataType = Utils.get_cycle_make(df.columns)
        logger.info(f'Cycle make: {cycleMake}. Data type: {dataType}')

        if file_meta:
            df = self.__transform_unstructured_data(df, file_meta)
        elif cycleMake == Constants.MAKE_ARBIN and dataType == Constants.DATA_TYPE_TEST_DATA:
            df = self.__transform_arbin_test_data(df)
        elif cycleMake == Constants.MAKE_MACCOR and dataType == Constants.DATA_TYPE_TEST_DATA:
            df = self.__transform_maccor_test_data(df)

        df = self.__consolidate_temps(df)

        # Apply user defined transformation
        if self.user_transform_test_data:
            df = self.user_transform_test_data(df)

        self.test_data = df.astype(object)
        return df

    def transform_cycle_stats(self, data: pd.DataFrame) -> pd.DataFrame:
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
        df = Utils.drop_empty_rows(df)

        cycleMake, dataType = Utils.get_cycle_make(df.columns)
        logger.info(f'cycle make: {cycleMake}, data type: {dataType}')

        if cycleMake == Constants.MAKE_ARBIN and dataType == Constants.DATA_TYPE_CYCLE_STATS:
            df = self.__transform_arbin_cycle_stats(df)

        if cycleMake == Constants.MAKE_MACCOR and dataType == Constants.DATA_TYPE_CYCLE_STATS:
            df = self.__transform_maccor_cycle_stats(df)

        # Apply user defined transformation
        if self.user_transform_cycle_stats:
            df = self.user_transform_cycle_stats(df)

        self.cycle_stats = df.astype(object)
        return df

    def __transform_unstructured_data(self, df: pd.DataFrame, file_meta: dict) -> pd.DataFrame:
        """
        Transforms unstructured data with file_meta to conform to BattETL naming and data conventions
        1. Rename columns
        2. Concert units
        4. Convert data type
        5. Sort data
        Parameters
        ----------
        df : pandas.DataFrame
            The input DataFrame
        file_meta : dict
            Dictionary containing the meta data for the file.
        Returns
        -------
        df : pandas.DataFrame
            The transformed output DataFrame
        """
        logger.info('Transform unstructured data')

        # Check user defined column names exist
        if not file_meta.get('voltage_mv'):
            raise ValueError(
                f'Voltage column name does not exist: `voltage_mv`')
        if not file_meta.get('current_ma'):
            raise ValueError(
                f'Current column name does not exist: `current_ma`')
        if not file_meta.get('test_time_s') or file_meta.get('recorded_datetime'):
            raise ValueError(
                f'Required to have either `test_time_s` or `recorded_datetime` column!')

        # Remove the `pandas_read_csv_args` dictionary if it is there
        file_meta.pop("pandas_read_csv_args", None)

        # Rename columns
        columns_mappings = {}
        for column in file_meta:
            columns_mappings[file_meta[column]['column_name']] = column
        df = Utils.rename_df_columns(df, columnsMapping=columns_mappings)

        # Apply any transformations
        for column in file_meta:
            if file_meta.get(column).get('scaling_factor'):
                df[column] = df[column] * \
                    file_meta.get(column).get('scaling_factor')

        df = self.__convert_data_type(df)

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

        if 'test_time_s' in df.columns and len(df) > 0 and self.__timedelta_validation_check(df['test_time_s'][0]):
            df = Utils.convert_timedelta_to_seconds(df, 'test_time_s')
        if 'step_time_s' in df.columns and len(df) > 0 and self.__timedelta_validation_check(df['step_time_s'][0]):
            df = Utils.convert_timedelta_to_seconds(df, 'step_time_s')

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

        if 'test_time_s' in df.columns and len(df) > 0 and self.__timedelta_validation_check(df['test_time_s'][0]):
            df = Utils.convert_timedelta_to_seconds(df, 'test_time_s')

        df = Utils.sort_dataframe(df, ['cycle'])

        return df

    def __timedelta_validation_check(self, input_string):
        # Check if it is like the format "1d 15:07:52.77" or "1d 15:07:52"
        regex = re.compile(r'\d+d \d+:\d+:\d+(\.\d+)?\Z', re.I)
        match = regex.match(str(input_string))
        return bool(match)

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

        # convert temperature columns to float
        temperature_columns = df.columns[
            df.columns.str.contains('thermocouple_')]
        if len(temperature_columns) > 0:
            logger.info('Convert temperature columns to float')
            for column in temperature_columns:
                df[column] = df[column].apply(Utils.convert_to_float)
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
        if 'arbin_charge_capacity_mah' in df:
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
        elif 'maccor_capacity_mah' in df:
            df['charge_capacity_mah'] = df[df.step.isin(
                steps['chg'])].maccor_capacity_mah
            df['discharge_capacity_mah'] = df[df.step.isin(
                steps['dsg'])].maccor_capacity_mah
            if 'maccor_energy_mwh' in df:
                df['charge_energy_mwh'] = df[df.step.isin(
                    steps['chg'])].maccor_energy_mwh
                df['discharge_energy_mwh'] = df[df.step.isin(
                    steps['dsg'])].maccor_energy_mwh
                logger.debug(
                    f'Harmonized capacity/energy values for Maccor cyclers. Modified {len(df)} rows and 4 columns.')
                logger.debug(
                    'Added columns: charge_capacity_mah, charge_energy_mwh, discharge_capacity_mah, discharge_energy_mwh')
            else:
                logger.debug(
                    f'Harmonized capacity values for Maccor cyclers. Modified {len(df)} rows and 2 columns.')
                logger.debug(
                    'Added columns: charge_capacity_mah, discharge_capacity_mah')
        else:
            logger.warning("No capacity columns were found to refactor!")

        return df

    def calc_cycle_stats(self, steps: dict, cv_voltage_threshold_mv: float = None, cell_thermocouple: int = None) -> pd.DataFrame:
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
        cell_thermocouple : int
            The number (as listed in the db column) of the thermocouple  that's attached to the cell.

        Returns
        -------
        self.cycle_stats : pd.DataFrame
            The calculated cycle statistics for all cycles.
        """
        if self.test_data.empty:
            logger.error("Cannot run `calc_cycle_stats()` without test_data!")
            return self.cycle_stats

        self.test_data = self.__harmonize_capacity(self.test_data, steps)

        # DataFrame where we will hold calculated cycle statistics for all cycles
        df_calced_stats = pd.DataFrame(columns=['cycle'])

        cycle_list = self.test_data['cycle'].unique()
        logger.info(
            f'Calculating cycle statistics for {len(cycle_list)} cycles')

        for cycle in cycle_list:
            cycle_data = self.test_data[self.test_data.cycle == cycle]
            if cycle_data.empty:
                logger.info("No cycle data for cycle " + str(cycle))
                continue

            # Calculate various charge and discharge cycle statistics.
            stats = {'cycle': cycle}

            charge_metrics = self.__calc_charge_stats(
                cycle_data, steps['chg'], cv_voltage_threshold_mv, cell_thermocouple)
            stats.update(charge_metrics)

            discharge_metrics = self.__calc_discharge_stats(
                cycle_data, steps['dsg'], cell_thermocouple)
            stats.update(discharge_metrics)

            # Calculate coulombic efficiency from the charge and discharge stats.
            if (('calculated_charge_capacity_mah' not in stats) or
                    ('calculated_discharge_capacity_mah' not in stats) or
                    (stats['calculated_charge_capacity_mah'] == 0)):
                ce = float("nan")
                logger.info(
                    "Unable to calculate coulombic efficiency for cycle " + str(cycle))
            else:
                ce = (discharge_metrics['calculated_discharge_capacity_mah']
                      / charge_metrics['calculated_charge_capacity_mah'])
            stats.update({'calculated_coulombic_efficiency': ce})

            # Append the cycle statistics from this cycle to our cumulative DataFrame for all cycles.
            df_calced_stats = pd.concat(
                [df_calced_stats, pd.DataFrame(stats, index=[0])], axis=0)

        if self.cycle_stats.empty:
            self.cycle_stats = df_calced_stats
        else:
            self.cycle_stats = self.cycle_stats.join(
                df_calced_stats.set_index('cycle'), on='cycle')

        return self.cycle_stats

    def __calc_charge_stats(self, cycle_data: pd.DataFrame, charge_steps: list, cv_voltage_threshold_mv: float = None, cell_thermocouple: int = None) -> dict:
        """
        Calculates various charge stats for a single cycle of data.

        Parameters
        ----------
        cycle_data : pd.DataFrame
            A DataFrame containing data for single battery cycle.
        charge_steps : list
            List of charge steps from the cycler schedule.
        cv_voltage_thresh_mv : float
            The the voltage threshold in milli-volts above which charge is considered to be constant voltage.
        cell_thermocouple : int
            The number (as listed in the db column) of the thermocouple  that's attached to the cell.

        Returns
        -------
        stats : dict
            A dictionary containing various charge stats.
        """
        logger.debug(
            f'Calculating charge statistics for cycle {cycle_data.cycle.iloc[0]} \
                with {len(cycle_data)} rows and {len(charge_steps)} charge steps.')

        if cv_voltage_threshold_mv:
            logger.debug(
                f'Using CV voltage threshold of {cv_voltage_threshold_mv} mV.')

        stats = {}

        # Define charge data to be where the step is a charge step.
        chg_data = cycle_data[cycle_data.step.isin(charge_steps)]
        if len(chg_data) < 2:
            logger.info("No charge data for cycle " +
                        str(cycle_data.cycle.iloc[0]))
            return stats

        ez_df = self.__ez_calc_df(
            chg_data, charge_steps, 'charge', cv_voltage_threshold_mv)

        stats['calculated_charge_capacity_mah'] = ez_df['charge_capacity_mah'].iloc[-1]
        stats['calculated_charge_energy_mwh'] = ez_df['charge_energy_mwh'].iloc[-1]
        stats['calculated_charge_time_s'] = ez_df['elapsed_time_s'].iloc[-1]

        stats['calculated_cc_charge_time_s'] = ez_df.cc_time_s.sum()
        stats['calculated_cv_charge_time_s'] = ez_df.cv_time_s.sum()
        stats['calculated_cc_capacity_mah'] = ez_df.cc_capacity_mah.sum()
        stats['calculated_cv_capacity_mah'] = ez_df.cv_capacity_mah.sum()

        # Calculate 50%/80% charge time & capacity.
        eighty_percent_cap_mah = stats['calculated_charge_capacity_mah'] * 0.8
        fifty_percent_cap_mah = stats['calculated_charge_capacity_mah'] * 0.5
        try:
            eighty_percent_cap_time_s = (ez_df[ez_df.charge_capacity_mah > eighty_percent_cap_mah].elapsed_time_s.iloc[0]
                                         - ez_df.elapsed_time_s.iloc[0])
            half_percent_cap_time_s = (ez_df[ez_df.charge_capacity_mah > fifty_percent_cap_mah].elapsed_time_s.iloc[0]
                                       - ez_df.elapsed_time_s.iloc[0])
        except:
            eighty_percent_cap_time_s = float('nan')
            half_percent_cap_time_s = float('nan')
            logger.warning(
                f'Incomplete charge data for cycle {cycle_data.cycle.iloc[0]}')

        stats['calculated_fifty_percent_charge_time_s'] = half_percent_cap_time_s
        stats['calculated_eighty_percent_charge_time_s'] = eighty_percent_cap_time_s

        if cell_thermocouple:
            logger.debug(
                f'Using cell thermocouple {cell_thermocouple} to calculate max charge temp.')
            thermocouple_col = f"thermocouple_{cell_thermocouple}_c"
            try:
                stats['calculated_max_charge_temp_c'] = chg_data.loc[:,
                                                                     thermocouple_col].max()
            except:
                logger.warning(
                    'cell_thermocouple value supplied, but not found in test_data.')

        return stats

    def __calc_discharge_stats(self, cycle_data: pd.DataFrame, discharge_steps: list, cell_thermocouple: int = None) -> dict:
        """
        Calculates various discharge stats for a single cycle of data.

        Parameters
        ----------
        cycle_data : pd.DataFrame
            A DataFrame containing data for single battery cycle.
        discharge_steps : list
            List of discharge steps from the cycler schedule.
        cell_thermocouple : int
            The number (as listed in the db column) of the thermocouple  that's attached to the cell.

        Returns
        -------
        stats : dict
            A dictionary containing various discharge stats.
        """
        logger.debug(
            f'Calculating discharge statistics for cycle {cycle_data.cycle.iloc[0]} \
                with {len(cycle_data)} rows and {len(discharge_steps)} discharge steps')
        stats = {}

        # Define discharge data to be where the step is a discharge step.
        dsg_data = cycle_data[cycle_data.step.isin(discharge_steps)]
        if len(dsg_data) < 2:
            logger.info("No discharge data for cycle " +
                        str(cycle_data.cycle.iloc[0]))
            return stats

        ez_df = self.__ez_calc_df(dsg_data, discharge_steps, 'discharge')

        stats['calculated_discharge_capacity_mah'] = ez_df['discharge_capacity_mah'].iloc[-1]
        stats['calculated_discharge_energy_mwh'] = ez_df['discharge_energy_mwh'].iloc[-1]
        stats['calculated_discharge_time_s'] = ez_df['elapsed_time_s'].iloc[-1]

        if cell_thermocouple:
            logger.debug(
                f'Using cell thermocouple {cell_thermocouple} to calculate max discharge temp.')
            thermocouple_col = f"thermocouple_{cell_thermocouple}_c"
            try:
                stats['calculated_max_discharge_temp_c'] = dsg_data.loc[:,
                                                                        thermocouple_col].max()
            except:
                logger.warning(
                    'cell_thermocouple value supplied, but not found in test_data.')
        return stats

    def __ez_calc_df(self, cycle_df: pd.DataFrame, steps: list, step_type: str, cv_voltage_thresh_mv: float = None) -> pd.DataFrame:
        """
        Creates a DataFrame we can easily use to calculate cycle statistics.

        Modifies test_data to cumulative capacity in cases where capacity is reset at each step.

        Parameters
        ----------
        df : pd.DataFrame
            The DataFrame use to calculate cumulative capacity.
        steps : list
            A list of the steps to calculate cumulative capacity for. 
        step_type : str
            Either 'charge' or 'discharge' depending on what type of steps we are calculating for.
        cv_voltage_thresh_mv : float
            The voltage threshold in mV above which charge is considered to be constant voltage.    

        Returns
        -------
        ez_df: pd.DataFrame
            DataFrame containing values filtered to charge steps with cumulative capacity
            and elapsed time calculated.
        """
        logger.debug(
            f'Calculating cumulative capacity with {len(cycle_df)} rows, \
                  {len(steps)} {step_type} steps and cv_voltage_thresh_mv: {cv_voltage_thresh_mv}')

        time_col = 'elapsed_time_s'
        volt_col = 'voltage_mv'
        cc_time = 'cc_time_s'
        cv_time = 'cv_time_s'
        cc_cap = 'cc_capacity_mah'
        cv_cap = 'cv_capacity_mah'
        if step_type == 'charge':
            cap_col = 'charge_capacity_mah'
            eng_col = 'charge_energy_mwh'
        elif step_type == 'discharge':
            cap_col = 'discharge_capacity_mah'
            eng_col = 'discharge_energy_mwh'
        else:
            logger.error(f'Unknown step type {step_type}!')
            return pd.DataFrame()

        ez_df = pd.DataFrame(
            columns=[time_col, volt_col, cap_col, eng_col, cc_time, cv_time, cc_cap, cv_cap])

        # filter step slice to charge/discharge steps
        relevant_steps = [step for step in cycle_df.step.unique()
                          if step in steps]

        # Iterate through each charge step to calculate cumulative capacity
        for step in relevant_steps:
            step_slice = cycle_df[cycle_df.step == step]

            if step_slice.empty:
                continue
            step_df = pd.DataFrame(
                columns=[time_col, volt_col, cap_col, eng_col, cc_time, cv_time, cc_cap, cv_cap])

            # Checks if we've processed at least one step already
            if not ez_df.empty:
                # Keeps running time across steps. Ignoring time between non-charge/discharge steps
                step_df[time_col] = \
                    (step_slice['test_time_s'] - step_slice['test_time_s'].iloc[0]) + ez_df[time_col].iloc[-1]
                
                # This catches if capacity was reset after each step. Modifies test_data DF in place.
                if step_slice[cap_col].iloc[0] < ez_df[cap_col].iloc[-1]:
                    current_cycle = cycle_df.cycle.iloc[0]
                    self.test_data.loc[
                        (self.test_data.cycle == current_cycle) &
                        (self.test_data.step == step),
                        cap_col] += ez_df[cap_col]
                    step_slice[cap_col] += ez_df[cap_col].iloc[-1]

                    if eng_col in self.test_data.columns:
                        self.test_data.loc[
                            (self.test_data.cycle == current_cycle) &
                            (self.test_data.step == step),
                            eng_col] += ez_df[eng_col]
                        step_slice[eng_col] += ez_df[eng_col].iloc[-1]
            else:
                step_df[time_col] = step_slice['test_time_s'] - \
                    step_slice['test_time_s'].iloc[0]

            # Step_slice is updated with above updates to test_data because it's a slice, not copy, of test_data
            step_df[volt_col] = step_slice[volt_col]
            step_df[cap_col] = step_slice[cap_col]
            if eng_col in step_slice.columns:
                step_df[eng_col] = step_slice[eng_col]

            if step_type == 'charge' and cv_voltage_thresh_mv is not None:
                latest_capacity = ez_df[cap_col].iloc[-1] if not ez_df.empty else 0

                # Calculate the delta time/cap for each step, sum the deltas in calc_stats() to get cycle charge time/cap
                delta_time = step_slice['step_time_s'] - \
                    step_slice['step_time_s'].shift(1, fill_value=0)
                step_df[cc_time] = np.where(
                    step_slice[volt_col] < cv_voltage_thresh_mv, delta_time, 0)
                step_df[cv_time] = np.where(
                    step_slice[volt_col] >= cv_voltage_thresh_mv, delta_time, 0)
                delta_cap = step_slice[cap_col] - step_slice[cap_col].shift(
                    1, fill_value=latest_capacity)
                step_df[cc_cap] = np.where(
                    step_slice[volt_col] < cv_voltage_thresh_mv, delta_cap, 0)
                step_df[cv_cap] = np.where(
                    step_slice[volt_col] >= cv_voltage_thresh_mv, delta_cap, 0)
            elif step_type == 'discharge':
                step_df[[cc_time, cc_cap, cv_time, cv_cap]] = np.nan
            ez_df = pd.concat([ez_df, step_df])

        return ez_df

    def __consolidate_temps(self, df):
        '''
        Adds thermocouple readings from each data point to an array containing all thermocouple readings.

        Parameters
        ----------
        df : pandas.DataFrame
            DataFrame containing test data.

        Returns
        -------
        pd.DataFrame
            DataFrame containing test data with thermocouple readings in an array.
        '''
        thermocouple_cols = [
            col for col in df.columns if re.search('thermocouple_\d+_c', col)]

        df['thermocouple_temps_c'] = df.apply(
            lambda row: [row[col] for col in thermocouple_cols], axis=1)

        return df
