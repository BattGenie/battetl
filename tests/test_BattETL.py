import os
import json
import pytest
import pandas as pd

from battetl import BattETL
from battetl.load import BattDbTestHelper

CONFIG_DIR = os.path.join(os.path.dirname(__file__), 'configs')


class Values:
    """
    Class to store values that are used in multiple tests

    Attributes
    ----------
    cell : BattETL
        The BattETL object that is used in the tests
    CONFIG_1 : dict
        The configuration dictionary for the test
    TEST_HELPER : BattDbTestHelper
        The BattDbTestHelper object that is used in the tests
    """
    cell: BattETL = None
    CONFIG_1: dict = None
    TEST_HELPER: BattDbTestHelper = None


@pytest.fixture(scope='module', autouse=True)
def add_database_test_entries():
    """
    Fixture to add test entries to the database before the tests are run
    and delete them after the tests are run
    """
    with open(os.path.join(CONFIG_DIR, 'config_1.json')) as config_file:
        Values.CONFIG_1 = json.load(config_file)

    Values.TEST_HELPER = BattDbTestHelper(Values.CONFIG_1)

    # Will be executed before the first test
    Values.TEST_HELPER.create_test_db_entries()
    yield Values.TEST_HELPER
    # Will be executed after the last test
    Values.TEST_HELPER.delete_test_db_entries()


@pytest.mark.battetl
class TestBattetl:
    def test_config(self, cell_config):
        config_path = os.path.join(
            os.path.dirname(__file__),
            'configs',
            'config_1.json'
        )

        Values.cell = BattETL(config_path)
        assert Values.cell.config == cell_config

    def test_extract(self):
        Values.cell.extract()
        assert Values.cell.schedule['file_name'] == 'tests/data/maccor_cycler_data/simple_data/BG_Maccor_Schedule.000'

    def test_transform(self):
        Values.cell.transform()
        assert not pd.DataFrame().equals(Values.cell.test_data)
        assert not pd.DataFrame().equals(Values.cell.cycle_stats)

    def test_transformed_test_data(self, data_column, data_row):
        # Maccor test data columns
        assert all(
            [col in Values.cell.test_data.columns for col in data_column.maccor.test_data]
        )

        # First row of transformed data
        row = Values.cell.test_data.iloc[0]
        assert row.equals(data_row.maccor.test_data_harmonized)

    def test_transformed_cycle_stats(self, data_column, data_row):
        # Maccor cycle stats columns
        assert all(
            [col in Values.cell.cycle_stats.columns for col in data_column.maccor.cycle_stats]
        )

        # First row of transformed data
        row = Values.cell.cycle_stats.iloc[0]
        assert row.equals(data_row.maccor.cycle_stats_calced)

    def test_load(self):
        Values.cell.load()

        db_last_row_test_data = Values.TEST_HELPER.read_last_row(
            'test_data', 'test_data_id')
        db_last_row_cycle_stats = Values.TEST_HELPER.read_last_row(
            'test_data_cycle_stats', 'cycle_stats_id')
        db_last_row_cells_meta = Values.TEST_HELPER.read_last_row(
            'cells_meta', 'cell_type_id')

        # Check that the voltage_mv is the same as the last row of the test_data table
        assert float(db_last_row_test_data[7]) == 4186.39, \
            'voltage_mv should equal 4186.39'

        # Check that the maccor_energy_mwh is the same as the last row of the test_data table
        assert 'maccor_energy_mwh' in db_last_row_test_data[-1], \
            'test_data other_details should have maccor_energy_mwh'

        # Check that the reported_charge_capacity_mah is the same as the last row of the cycle_stats table
        assert float(db_last_row_cycle_stats[4]) == 2743.710761367, \
            'reported_charge_capacity_mah should equal 2743.710761367'

        # Check that the maccor_charge_thermocouple_max_c is the same as the last row of the cycle_stats table
        assert 'maccor_charge_thermocouple_max_c' in db_last_row_cycle_stats[-1], \
            'cycle_stats other_details should have maccor_charge_thermocouple_max_c'

        # Check that the datasheet is load correctly
        with open(Values.cell.config['meta_data']['cell_meta']['datasheet'], 'rb') as f:
            datasheet_data = f.read()
        assert datasheet_data == db_last_row_cells_meta[7].tobytes(), \
            'datasheet should be load correctly'

        Values.TEST_HELPER.delete_test_data()
