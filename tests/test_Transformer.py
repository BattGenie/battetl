import os
import pytest
from os.path import join

from battetl.extract import Extractor
from battetl.transform import Transformer

BASE_DATA_PATH = os.path.join(os.path.dirname(__file__), 'data')
MACCOR_PATH = os.path.join(BASE_DATA_PATH, 'maccor_cycler_data')
MACCOR_SIMPLE_PATH = os.path.join(MACCOR_PATH, 'simple_data')
MACCOR_TYPE2_PATH = os.path.join(MACCOR_PATH, 'type2_data')
ARBIN_PATH = os.path.join(BASE_DATA_PATH, 'arbin_cycler_data')
ARBIN_SINGLE_PATH = os.path.join(ARBIN_PATH, 'single_data_file')
ARBIN_MULTIPLE_PATH = os.path.join(ARBIN_PATH, 'multiple_data_files')
ICT_PATH = os.path.join(ARBIN_PATH, 'ict_data_files')
CCCV_PATH = os.path.join(ARBIN_PATH, 'cccv_data_files')
RESET_CAP_PATH = os.path.join(MACCOR_PATH, 'reset_cap_data')
STEP_ORDER_PATH = os.path.join(ARBIN_PATH, 'step_order_data_files')


@pytest.mark.transform
@pytest.mark.maccor
def test_transform_maccor_test_data(data_column, data_row):
    path = join(MACCOR_SIMPLE_PATH, 'BG_Maccor_TestData - 079.pkl')
    extractor = Extractor()
    df_extracted = extractor.from_pickle(path)

    transformer = Transformer()
    transformer.transform_test_data(df_extracted)
    df_transformed = transformer.test_data

    # Maccor test data columns
    assert all(
        [col in df_transformed.columns for col in data_column.maccor.test_data]
    )

    # First row of transformed data
    row = df_transformed.iloc[0]
    assert row.equals(data_row.maccor.test_data)


@pytest.mark.transform
@pytest.mark.maccor
def test_transform_maccor_test_data_type2(data_column, data_row):
    path = join(MACCOR_TYPE2_PATH, 'BG_Maccor_Type2 - 075.pkl')
    extractor = Extractor()
    df_extracted = extractor.from_pickle(path)

    transformer = Transformer()
    transformer.transform_test_data(df_extracted)
    df_transformed = transformer.test_data

    # Maccor test data columns
    assert all(
        [col in df_transformed.columns for col in data_column.maccor.test_data_type2]
    )

    # First row of transformed data
    row = df_transformed.iloc[0]
    assert row.equals(data_row.maccor.test_data_type2)


@pytest.mark.transform
@pytest.mark.maccor
def test_transform_maccor_cycle_stats(data_column, data_row):
    path = join(MACCOR_SIMPLE_PATH, 'BG_Maccor_TestData - 079 [STATS].pkl')
    extractor = Extractor()
    df_extracted = extractor.from_pickle(path)

    transformer = Transformer()
    transformer.transform_cycle_stats(df_extracted)
    df_transformed = transformer.cycle_stats

    # Maccor cycle stats columns
    assert all(
        [col in df_transformed.columns for col in data_column.maccor.cycle_stats]
    )

    # First row of transformed data
    row = df_transformed.iloc[0]
    assert row.equals(data_row.maccor.cycle_stats)


@pytest.mark.transform
@pytest.mark.arbin
def test_transform_arbin_test_data(data_column, data_row):
    path = join(ARBIN_SINGLE_PATH,
                'BG_Arbin_TestData_Single_File_Channel_26_Wb_1.pkl')
    extractor = Extractor()
    df_extracted = extractor.from_pickle(path)

    transformer = Transformer()
    transformer.transform_test_data(df_extracted)
    df_transformed = transformer.test_data

    # Maccor test data columns
    assert all(
        [col in df_transformed.columns for col in data_column.arbin.test_data]
    )

    # First row of transformed data
    row = df_transformed.iloc[0]
    assert row.equals(data_row.arbin.test_data)


@pytest.mark.transform
@pytest.mark.arbin
def test_transform_arbin_cycle_stats(data_column, data_row):
    path = join(ARBIN_SINGLE_PATH,
                'BG_Arbin_TestData_Single_File_Channel_26_StatisticByCycle.pkl')
    extractor = Extractor()
    df_extracted = extractor.from_pickle(path)

    transformer = Transformer()
    transformer.transform_cycle_stats(df_extracted)
    df_transformed = transformer.cycle_stats

    # Maccor cycle stats columns
    assert all(
        [col in df_transformed.columns for col in data_column.arbin.cycle_stats]
    )

    # First row of transformed data
    row = df_transformed.iloc[0]
    assert row.equals(data_row.arbin.cycle_stats)


@pytest.mark.transform
@pytest.mark.maccor
def test_user_defined_test_data_transformation(data_column, data_row):
    path = join(MACCOR_SIMPLE_PATH, 'BG_Maccor_TestData - 079.pkl')
    extractor = Extractor()
    df_extracted = extractor.from_pickle(path)

    # User defined test data transformation
    def user_transform_test_data(df):
        df['voltage_v'] = df['voltage_mv'] * 1000
        return df

    transformer = Transformer(
        user_transform_test_data=user_transform_test_data)
    transformer.transform_test_data(df_extracted)
    df_transformed = transformer.test_data

    # Maccor test data columns
    assert all(
        [col in df_transformed.columns for col in data_column.maccor.test_data + ['voltage_v']]
    )

    # First row of transformed data
    row = df_transformed.iloc[0]
    maccor_test_data = data_row.maccor.test_data
    maccor_test_data['voltage_v'] = maccor_test_data['voltage_mv'] * 1000
    assert row.equals(maccor_test_data)


@pytest.mark.transform
@pytest.mark.maccor
def test_user_defined_cycle_stats_transformation(data_column, data_row):
    path = join(MACCOR_SIMPLE_PATH, 'BG_Maccor_TestData - 079 [STATS].pkl')
    extractor = Extractor()
    df_extracted = extractor.from_pickle(path)

    # User defined cycle stats transformation
    def user_transform_cycle_stats(df):
        df['test_time_m'] = df['test_time_s'] / 60
        return df

    transformer = Transformer(
        user_transform_cycle_stats=user_transform_cycle_stats)
    transformer.transform_cycle_stats(df_extracted)
    df_transformed = transformer.cycle_stats

    # Maccor cycle stats columns
    assert all(
        [col in df_transformed.columns for col in data_column.maccor.cycle_stats + ['test_time_m']]
    )

    # First row of transformed data
    row = df_transformed.iloc[0]
    maccor_cycle_stats = data_row.maccor.cycle_stats
    maccor_cycle_stats['test_time_m'] = maccor_cycle_stats['test_time_s'] / 60
    assert row.equals(maccor_cycle_stats)


@pytest.mark.transform
@pytest.mark.maccor
@pytest.mark.stats
def test_calc_maccor_cycle_stats():
    data_path = join(MACCOR_SIMPLE_PATH, 'BG_Maccor_TestData - 079.pkl')
    stat_path = join(MACCOR_SIMPLE_PATH,
                     'BG_Maccor_TestData - 079 [STATS].pkl')
    schedule_path = join(MACCOR_SIMPLE_PATH, 'BG_Maccor_Schedule.000')

    extractor = Extractor()
    df_data_raw = extractor.from_pickle(data_path)
    df_stats_raw = extractor.from_pickle(stat_path)
    schedule = extractor.schedule_from_files([schedule_path])

    transformer = Transformer()
    transformer.transform_test_data(df_data_raw)
    transformer.transform_cycle_stats(df_stats_raw)

    transformer.calc_cycle_stats(
        schedule['steps'], cv_voltage_threshold_mv=4195)

    # Make sure reported/calculated charge and discharge capacity are within 1% of each other.
    assert ((0.01 > abs(transformer.cycle_stats.reported_discharge_capacity_mah /
                        transformer.cycle_stats.calculated_discharge_capacity_mah - 1).iloc[1:-1])).all()

    assert ((0.01 > abs(transformer.cycle_stats.reported_charge_capacity_mah /
                        transformer.cycle_stats.calculated_charge_capacity_mah - 1).iloc[1:-1])).all()


@pytest.mark.transform
@pytest.mark.arbin
@pytest.mark.stats
def test_calc_arbin_cycle_stats():
    data_path = join(ARBIN_SINGLE_PATH,
                     'BG_Arbin_TestData_Single_File_Channel_26_Wb_1.pkl')
    stat_path = join(
        ARBIN_SINGLE_PATH, 'BG_Arbin_TestData_Single_File_Channel_26_StatisticByCycle.pkl')
    schedule_path = join(
        ARBIN_SINGLE_PATH, 'BG_25R_Characterization+BG_25R.sdx')

    extractor = Extractor()
    df_data_raw = extractor.from_pickle(data_path)
    df_stats_raw = extractor.from_pickle(stat_path)
    schedule = extractor.schedule_from_files([schedule_path])

    transformer = Transformer()
    transformer.transform_test_data(df_data_raw)
    transformer.transform_cycle_stats(df_stats_raw)

    transformer.calc_cycle_stats(
        schedule['steps'], cv_voltage_threshold_mv=4195)

    # Make sure reported/calculated charge and discharge capacity are within 1% of each other.
    assert ((0.01 > abs(transformer.cycle_stats.reported_discharge_capacity_mah /
                        transformer.cycle_stats.calculated_discharge_capacity_mah - 1).iloc[1:-1])).all()

    assert ((0.01 > abs(transformer.cycle_stats.reported_charge_capacity_mah /
                        transformer.cycle_stats.calculated_charge_capacity_mah - 1).iloc[1:-1])).all()


@pytest.mark.transform
@pytest.mark.arbin
@pytest.mark.stats
def test_ict_data():
    data_path = join(
        ICT_PATH, 'BG_Arbin_TestData_ICT_Cell3_Channel_4_Wb_1.CSV')
    stat_path = join(
        ICT_PATH, 'BG_Arbin_TestData_ICT_Cell3_Channel_4_StatisticByCycle.CSV')
    schedule_path = join(
        ICT_PATH, 'BG_Arbin_TestData_ICT+BG_Arbin_TestData_xxxxx.sdx')

    extractor = Extractor()
    extractor.data_from_files([data_path])
    extractor.data_from_files([stat_path])
    schedule = extractor.schedule_from_files([schedule_path])

    transformer = Transformer()
    transformer.transform_test_data(extractor.raw_test_data)
    transformer.transform_cycle_stats(extractor.raw_cycle_stats)

    transformer.calc_cycle_stats(
        schedule['steps'], cv_voltage_threshold_mv=4195)

    # Make sure reported/calculated stats are within 1% of each other.
    # TODO: Add additional criteria for ICT data (charge_time, discharge_time, etc.)
    assert ((0.01 > abs(transformer.cycle_stats.reported_discharge_capacity_mah /
                        transformer.cycle_stats.calculated_discharge_capacity_mah - 1).iloc[1:-1])).all()
    assert ((0.01 > abs(transformer.cycle_stats.reported_charge_capacity_mah /
                        transformer.cycle_stats.calculated_charge_capacity_mah - 1).iloc[1:-1])).all()


@pytest.mark.transform
@pytest.mark.arbin
@pytest.mark.stats
def test_cccv_data():
    import pandas as pd

    data_path = join(
        CCCV_PATH, 'BG_Arbin_TestData_CCCV_Cell_2_Channel_26_Wb_1.CSV')
    stat_path = join(
        CCCV_PATH, 'BG_Arbin_TestData_CCCV_Cell_2_Channel_26_StatisticByCycle.CSV')
    schedule_path = join(
        CCCV_PATH, 'BG_Arbin_Cell_TestData_CCCV+BG_Arbin_Cell.sdx')

    extractor = Extractor()
    extractor.data_from_files([data_path])
    extractor.data_from_files([stat_path])
    schedule = extractor.schedule_from_files([schedule_path])

    transformer = Transformer()
    transformer.transform_test_data(extractor.raw_test_data)
    transformer.transform_cycle_stats(extractor.raw_cycle_stats)

    transformer.calc_cycle_stats(
        schedule['steps'], cv_voltage_threshold_mv=4200)

    true_values = pd.DataFrame(
        columns=['cycle', 'cc_cap', 'cv_cap', 'cc_time', 'cv_time'])
    true_values['cycle'] = pd.Series([1, 2, 3])
    true_values['cc_cap'] = pd.Series([0, 2567.811, 2561.239])
    true_values['cv_cap'] = pd.Series([0, 6.2E-02, 2.743])
    true_values['cc_time'] = pd.Series([0, 73971.1505, 36904.3838])
    true_values['cv_time'] = pd.Series([0, 1.7623, 60.6568])

    # Make sure calculated CC-CV stats are within 1% of actual value.
    assert ((0.01 > abs(transformer.cycle_stats.calculated_cc_charge_time_s /
                        true_values['cc_time'] - 1).fillna(0))).all()
    assert ((0.01 > abs(transformer.cycle_stats.calculated_cv_charge_time_s /
                        true_values['cv_time'] - 1).fillna(0))).all()
    assert ((0.01 > abs(transformer.cycle_stats.calculated_cc_capacity_mah /
                        true_values['cc_cap'] - 1).fillna(0))).all()
    assert ((0.01 > abs(transformer.cycle_stats.calculated_cv_capacity_mah /
                        true_values['cv_cap'] - 1).fillna(0))).all()

    # Make sure sum of CC-CV values are within 1% of reported total values.
    # Ignore first row because it just has CC step.
    assert ((0.01 > abs(transformer.cycle_stats.reported_charge_time_s.iloc[1:] /
                        (true_values['cc_time'].iloc[1:] + true_values['cv_time'].iloc[1:]) - 1))).all()
    assert ((0.01 > abs(transformer.cycle_stats.reported_charge_capacity_mah.iloc[1:] /
                        (true_values['cc_cap'].iloc[1:] + true_values['cv_cap'].iloc[1:]) - 1))).all()


@pytest.mark.transform
@pytest.mark.arbin
@pytest.mark.stats
def test_capacity_reset_on_step():
    data_path = join(
        RESET_CAP_PATH, 'BG_Maccor_Cell_ResetCap_Take11 - 074.txt')
    stat_path = join(
        RESET_CAP_PATH, 'BG_Maccor_Cell_ResetCap_Take11 - 074 [STATS].txt')
    schedule_path = join(
        RESET_CAP_PATH, 'BG_Maccor_ResetCap.000')

    extractor = Extractor()
    extractor.data_from_files([data_path])
    extractor.data_from_files([stat_path])
    schedule = extractor.schedule_from_files([schedule_path])

    transformer = Transformer()
    transformer.transform_test_data(extractor.raw_test_data)
    transformer.transform_cycle_stats(extractor.raw_cycle_stats)

    transformer.calc_cycle_stats(
        schedule['steps'], cv_voltage_threshold_mv=4429.5)

    test_cycle = 7
    cc_time = 1701.14
    cc_cap = 2120.64
    cv_time = 4541.32
    cv_cap = 1354.83
    discharge_time = 3447.77
    discharge_cap = 3474.54
    charge_time_50 = 1352.0
    charge_time_80 = 2555.99

    # Make sure calculated CC-CV stats are within 1% of actual value.
    assert (0.01 > abs(
        transformer.cycle_stats.at[test_cycle, 'calculated_cc_charge_time_s'] / cc_time - 1))
    assert (0.01 > abs(
        transformer.cycle_stats.at[test_cycle, 'calculated_cv_charge_time_s'] / cv_time - 1))
    assert (0.01 > abs(
        transformer.cycle_stats.at[test_cycle, 'calculated_cc_capacity_mah'] / cc_cap - 1))
    assert (0.01 > abs(
        transformer.cycle_stats.at[test_cycle, 'calculated_cv_capacity_mah'] / cv_cap - 1))
    assert (0.01 > abs(
        transformer.cycle_stats.at[test_cycle, 'calculated_discharge_time_s'] / discharge_time - 1))
    assert (0.01 > abs(
        transformer.cycle_stats.at[test_cycle, 'calculated_discharge_capacity_mah'] / discharge_cap - 1))

    # Make sure 50% and 80% charge times are within 1% of actual value.
    assert (0.1 > abs(
        transformer.cycle_stats.at[test_cycle, 'calculated_fifty_percent_charge_time_s'] / charge_time_50 - 1))
    assert (0.1 > abs(
        transformer.cycle_stats.at[test_cycle, 'calculated_eighty_percent_charge_time_s'] / charge_time_80 - 1))

    # Make sure reported/calculated stats are within 1% of each other.
    # Only look at discharge. Reported charge cap is off because it's reset at each step. Shows the cap from the last charge step.
    assert ((0.01 > abs(transformer.cycle_stats.reported_discharge_capacity_mah /
                        transformer.cycle_stats.calculated_discharge_capacity_mah - 1).iloc[1:-1])).all()


@pytest.mark.transform
@pytest.mark.arbin
@pytest.mark.stats
def test_max_temperature():
    import pandas as pd

    data_path = join(
        ARBIN_SINGLE_PATH, 'BG_Arbin_TestData_Single_File_Channel_26_Wb_1.pkl')
    stat_path = join(
        ARBIN_SINGLE_PATH, 'BG_Arbin_TestData_Single_File_Channel_26_StatisticByCycle.pkl')
    schedule_path = join(
        ARBIN_SINGLE_PATH, 'BG_25R_Characterization+BG_25R.sdx')

    extractor = Extractor()
    df_raw_test_data = extractor.from_pickle(data_path)
    df_raw_cycle_stats = extractor.from_pickle(stat_path)
    schedule = extractor.schedule_from_files([schedule_path])

    transformer = Transformer()
    transformer.transform_test_data(df_raw_test_data)
    transformer.transform_cycle_stats(df_raw_cycle_stats)

    transformer.calc_cycle_stats(
        schedule['steps'], cv_voltage_threshold_mv=4195, cell_thermocouple=2)

    max_temps = pd.DataFrame(
        data=[[1, None, 25.09362588],
              [2, 24.80076, 24.66491629],
              [3, 24.94959932, 25.34211339],
              [4, 25.09418, 26.08771662],
              [5, 26.97403, 28.22165],
              [6, 30.26020972, 31.67171],
              [7, 38.144, 39.89985],
              [8, 47.34957491, 48.98906797],
              [9, 30.24211, None]],
        columns=['cycle', 'max_charge_temp', 'max_discharge_temp'])

    # check that max temperatures are within 0.001 C of expected values
    assert (0.001 > abs(
        transformer.cycle_stats.loc[0:7, 'calculated_max_discharge_temp_c'] - max_temps.loc[0:7, 'max_discharge_temp'])).all()
    assert (0.001 > abs(
        transformer.cycle_stats.loc[1:8, 'calculated_max_charge_temp_c'] - max_temps.loc[1:8, 'max_charge_temp'])).all()


@pytest.mark.transform
@pytest.mark.arbin
@pytest.mark.stats
def test_step_order_stat_calcs():
    data_path = join(
        STEP_ORDER_PATH, 'BG_Arbin_MBC5v2_Cell_Cell6_Channel_25_Wb_1.csv')
    stat_path = join(
        STEP_ORDER_PATH, 'BG_Arbin_MBC5v2_25R_Cell6_Channel_25_StatisticByCycle.CSV')
    schedule_path = join(
        STEP_ORDER_PATH, 'BG_Arbin_MBC5_25R_3+BG_Arbin_25R.sdx')

    extractor = Extractor()
    extractor.data_from_files([data_path])
    extractor.data_from_files([stat_path])
    schedule = extractor.schedule_from_files([schedule_path])

    transformer = Transformer()
    transformer.transform_test_data(extractor.raw_test_data)
    transformer.transform_cycle_stats(extractor.raw_cycle_stats)

    transformer.calc_cycle_stats(
        schedule['steps'], cv_voltage_threshold_mv=4420)

    fifty_percent_charge_time, eighty_percent_charge_time = transformer.cycle_stats.loc[transformer.cycle_stats.cycle == 8, [
        'calculated_fifty_percent_charge_time_s', 'calculated_eighty_percent_charge_time_s']].values[0]

    assert (0.01 > abs(fifty_percent_charge_time / 614.07 - 1))
    assert (0.01 > abs(eighty_percent_charge_time / 1040.08 - 1))
