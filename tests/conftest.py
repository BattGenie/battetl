import pytest
import numpy as np
import pandas as pd


@pytest.fixture
def env_example():
    data = {
        'ENV': 'dev',
        'DB_HOSTNAME': 'localhost',
        'DB_PORT': '5432',
        'DB_TARGET': 'YOUR_DATABASE_NAME',
        'DB_USERNAME': 'YOUR_USERNAME',
        'DB_PASSWORD': 'YOUR_PASSWORD',
    }
    return data


@pytest.fixture
def cell_config():
    data = {
        "timezone": "America/Los_Angeles",
        "data_file_path": [
            "tests/data/maccor_cycler_data/simple_data/BG_Maccor_Testdata - 079.txt"
        ],
        "stats_file_path": [
            "tests/data/maccor_cycler_data/simple_data/BG_Maccor_TestData - 079 [STATS].txt"
        ],
        "schedule_file_path": [
            "tests/data/maccor_cycler_data/simple_data/BG_Maccor_Schedule.000"
        ],
        "meta_data": {
            "test_meta": {
                "test_name": "test_test",
                "channel": 10
            },
            "cell": {
                "manufacturer_sn": "0001"
            },
            "cell_meta": {
                "manufacturer": "FakeMN",
                "manufacturer_pn": "1234"
            },
            "schedule_meta": {
                "schedule_name": "fake_schedule.000",
                "cycler_make": "BattGenie"
            },
            "cycler": {
                "sn": "0001"
            },
            "cycler_meta": {
                "manufacturer": "BattGenie",
                "model": "Cycler9000"
            }
        }
    }
    return data


@pytest.fixture
def data_column():
    class data:
        class maccor:

            test_data = [
                'cycle',
                'step',
                'test_time_s',
                'step_time_s',
                'maccor_capacity_mah',
                'maccor_energy_mwh',
                'current_ma',
                'voltage_mv',
                'es',
                'recorded_datetime',
                'acr',
                'dcir',
                'thermocouple_1_c',
                'unixtime_s',
            ]

            test_data_type2 = [
                'rec',
                'cycle',
                'cycle c',
                'step',
                'test_time_s',
                'step_time_s',
                'capacity',
                'energy',
                'current_ma',
                'voltage_mv',
                'md',
                'es',
                'recorded_datetime',
                'unixtime_s',
            ]

            cycle_stats = [
                'cycle',
                'cycle type',
                'test_time_s',
                'maccor_min_current_ma',
                'maccor_min_voltage_mv',
                'reported_charge_capacity_mah',
                'reported_discharge_capacity_mah',
                'reported_charge_energy_mwh',
                'reported_discharge_energy_mwh',
                'acr_ohm',
                'dcir',
                'maccor_charge_thermocouple_start_c',
                'maccor_charge_thermocouple_end_c',
                'maccor_charge_thermocouple_min_c',
                'maccor_charge_thermocouple_max_c',
                'maccor_discharge_thermocouple_start_c',
                'maccor_discharge_thermocouple_end_c',
                'maccor_discharge_thermocouple_min_c',
                'maccor_discharge_thermocouple_max_c',
                'date',
            ]

        class arbin:
            test_data = [
                'data_point',
                'recorded_datetime',
                'test_time_s',
                'step_time_s',
                'cycle',
                'step',
                'current_ma',
                'voltage_mv',
                'power_mw',
                'arbin_charge_capacity_mah',
                'arbin_discharge_capacity_mah',
                'arbin_charge_energy_mwh',
                'arbin_discharge_energy_mwh',
                'acr (ohm)',
                'dv/dt (v/s)',
                'impedance_mohm',
                'dq/dv (ah/v)',
                'dv/dq (v/ah)',
                'thermocouple_1_c',
                'aux_dt/dt_1 (c/s)',
                'thermocouple_2_c',
                'aux_dt/dt_2 (c/s)',
                'unixtime_s',
            ]
            cycle_stats = [
                'recorded_datetime',
                'test_time_s',
                'step_time_s',
                'cycle',
                'step',
                'current_ma',
                'voltage_mv',
                'power (w)',
                'reported_charge_capacity_mah',
                'reported_discharge_capacity_mah',
                'reported_charge_energy_mwh',
                'reported_discharge_energy_mwh',
                'reported_charge_time_s',
                'reported_discharge_time_s',
                'v_max_on_cycle (v)',
                'reported_coulombic_efficiency',
                'mah/g',
                'thermocouple_1_c',
                'aux_dt/dt_1 (c/s)',
                'thermocouple_2_c',
                'aux_dt/dt_2 (c/s)',
            ]

    return data


@pytest.fixture
def data_row():
    class data:
        class maccor:
            test_data = pd.DataFrame([{
                'cycle': 0,
                'step': 1,
                'test_time_s': 0.0,
                'step_time_s': 0.0,
                'maccor_capacity_mah': 0.0,
                'maccor_energy_mwh': 0.0,
                'current_ma': 0.0,
                'voltage_mv': 4160.3,
                'es': 0,
                'recorded_datetime': pd.Timestamp('2020-03-27 06:00:14+00:00', tz='America/Los_Angeles').tz_convert('UTC'),
                'acr': 0.0,
                'dcir': 0.0,
                'thermocouple_1_c': 26.55,
                'unixtime_s': 1585288814,
            }]).iloc[0]

            test_data_type2 = pd.DataFrame([{
                'rec': '1',
                'cycle': 0,
                'cycle c': 1,
                'step': 1,
                'test_time_s': 0.0,
                'step_time_s': 0.0,
                'capacity': 0.0,
                'energy': 0.0,
                'current_ma': 0.0,
                'voltage_mv': 3887.0,
                'md': 'R',
                'es': 0,
                'recorded_datetime': pd.Timestamp('2023-03-25 18:03:38+00:00', tz='America/Los_Angeles').tz_convert('UTC'),
                'unixtime_s': 1679767418,
            }]).iloc[0]

            cycle_stats = pd.DataFrame([{
                'cycle': 0.0,
                'cycle type': np.nan,
                'test_time_s': 539.16,
                'maccor_min_current_ma': 0.099794003204,
                'maccor_min_voltage_mv': 4.200122072175,
                'reported_charge_capacity_mah': 36.577804451999995,
                'reported_discharge_capacity_mah': np.nan,
                'reported_charge_energy_mwh': 153.244853203,
                'reported_discharge_energy_mwh': np.nan,
                'acr_ohm': 0.0,
                'dcir': 0.0,
                'maccor_charge_thermocouple_start_c': 26.537836074829,
                'maccor_charge_thermocouple_end_c': 26.621141433716,
                'maccor_charge_thermocouple_min_c': 26.537836074829,
                'maccor_charge_thermocouple_max_c': 26.65446472168,
                'maccor_discharge_thermocouple_start_c': np.nan,
                'maccor_discharge_thermocouple_end_c': np.nan,
                'maccor_discharge_thermocouple_min_c': np.nan,
                'maccor_discharge_thermocouple_max_c': np.nan,
                'date': 43916.9647483565,
            }]).iloc[0]

        class arbin:
            test_data = pd.DataFrame([{
                'data_point': 1,
                'recorded_datetime': pd.Timestamp('2022-05-22 12:43:10.717000-07:00', tz='America/Los_Angeles').tz_convert('UTC'),
                'test_time_s': 5.0021,
                'step_time_s': 5.0021,
                'cycle': 1,
                'step': 1,
                'current_ma': 0.0,
                'voltage_mv': 3373.0280000000002,
                'power_mw': 0.0,
                'arbin_charge_capacity_mah': 0.0,
                'arbin_discharge_capacity_mah': 0.0,
                'arbin_charge_energy_mwh': 0.0,
                'arbin_discharge_energy_mwh': 0.0,
                'acr (ohm)': np.nan,
                'dv/dt (v/s)': np.nan,
                'impedance_mohm': np.nan,
                'dq/dv (ah/v)': np.nan,
                'dv/dq (v/ah)': np.nan,
                'thermocouple_1_c': 24.25,
                'aux_dt/dt_1 (c/s)': 0.0,
                'thermocouple_2_c': 24.64854,
                'aux_dt/dt_2 (c/s)': -0.002137546,
                'unixtime_s': 1653248590,
            }]).iloc[0]
            cycle_stats = pd.DataFrame([{
                'recorded_datetime': '\t05/22/2022 14:06:34.312',
                'test_time_s': 5008.5971,
                'step_time_s': 600.0023,
                'cycle': 1,
                'step': 4,
                'current_ma': 0.0,
                'voltage_mv': 2640.8360000000002,
                'power (w)': 0,
                'reported_charge_capacity_mah': 8.269678e-07,
                'reported_discharge_capacity_mah': 305.3377,
                'reported_charge_energy_mwh': 2.789791e-06,
                'reported_discharge_energy_mwh': 938.1328,
                'reported_charge_time_s': 0.0076,
                'reported_discharge_time_s': 4396.797,
                'v_max_on_cycle (v)': 0,
                'reported_coulombic_efficiency': 36922560000.0,
                'mah/g': np.nan,
                'thermocouple_1_c': 23.9370002746527,
                'aux_dt/dt_1 (c/s)': 0.0,
                'thermocouple_2_c': 24.7937816681912,
                'aux_dt/dt_2 (c/s)': 0.0010763981880476,
            }]).iloc[0]

    return data
