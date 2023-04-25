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

    cv_voltage_thresh_mv = 4195
    transformer.calc_cycle_stats(schedule['steps'], cv_voltage_thresh_mv)

    # Make sure reported/calculated charge and discharge capacity are within 1% of each other.
    assert ((1 > abs(transformer.cycle_stats.reported_discharge_capacity_mah /
                     transformer.cycle_stats.calculated_discharge_capacity_mah - 1).iloc[1:-1])).all()

    assert ((1 > abs(transformer.cycle_stats.reported_charge_capacity_mah /
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

    cv_voltage_thresh_mv = 4195
    transformer.calc_cycle_stats(schedule['steps'], cv_voltage_thresh_mv)

    # Make sure reported/calculated charge and discharge capacity are within 1% of each other.
    assert ((1 > abs(transformer.cycle_stats.reported_discharge_capacity_mah /
                     transformer.cycle_stats.calculated_discharge_capacity_mah - 1).iloc[1:-1])).all()

    assert ((1 > abs(transformer.cycle_stats.reported_charge_capacity_mah /
                     transformer.cycle_stats.calculated_charge_capacity_mah - 1).iloc[1:-1])).all()
