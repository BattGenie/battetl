import os
import json
import pytest
import pandas as pd
from copy import deepcopy

from battetl.load import Loader, BattDbTestHelper

CONFIG_DIR = os.path.join(os.path.dirname(__file__), 'configs')


class Values:
    """
    Class to store values that are used in multiple tests

    Attributes
    ----------
    CONFIG_1: dict
        The configuration dictionary for the test
    TEST_HELPER: BattDbTestHelper
        The BattDbTestHelper object that is used in the tests
    """
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


@pytest.mark.database
@pytest.mark.load
def test_lookup_cell_type_id():

    config = deepcopy(Values.CONFIG_1)

    # Lookup existing cell_type_id
    loader = Loader(config)
    cell_type_id = loader._Loader__lookup_cell_type_id()
    assert (cell_type_id == Values.TEST_HELPER.cell_type_id)
    del loader

    # Lookup non-existent cell_type_id
    config['meta_data']['cell_meta']['manufacturer'] = Values.TEST_HELPER.generate_random_string()
    config['meta_data']['cell_meta']['manufacturer_pn'] = Values.TEST_HELPER.generate_random_string()
    loader = Loader(config)
    cell_type_id = loader._Loader__lookup_cell_type_id()
    assert (cell_type_id is None)


@pytest.mark.database
@pytest.mark.load
def test_lookup_cell_id():

    config = deepcopy(Values.CONFIG_1)

    # Lookup existing cell id
    loader = Loader(config)
    cell_id = loader._lookup_cell_id()
    assert (cell_id == Values.TEST_HELPER.cell_id)
    del loader

    # Lookup non-existent cell_id
    config['meta_data']['cell']['manufacturer_sn'] = Values.TEST_HELPER.generate_random_string()
    config['meta_data']['cell']['label'] = Values.TEST_HELPER.generate_random_string()
    loader = Loader(config)
    cell_id = loader._lookup_cell_id()
    assert (cell_id is None)


@pytest.mark.database
@pytest.mark.load
def test_lookup_schedule_id():

    config = deepcopy(Values.CONFIG_1)

    # Lookup existing schedule id
    loader = Loader(config)
    schedule_id = loader._Loader__lookup_schedule_id()
    assert (schedule_id == Values.TEST_HELPER.schedule_id)
    del loader

    # Lookup non-existent schedule_id
    config['meta_data']['schedule_meta']['schedule_name'] = Values.TEST_HELPER.generate_random_string()
    loader = Loader(config)
    schedule_id = loader._Loader__lookup_schedule_id()
    assert (schedule_id is None)


@pytest.mark.database
@pytest.mark.load
def test_lookup_cycler_type_id():

    config = deepcopy(Values.CONFIG_1)

    # Lookup existing cycler type id
    loader = Loader(config)
    cycler_type_id = loader._Loader__lookup_cycler_type_id()
    assert (cycler_type_id == Values.TEST_HELPER.cycler_type_id)
    del loader

    # Lookup non-existent schedule_id
    config['meta_data']['cycler_meta']['manufacturer'] = Values.TEST_HELPER.generate_random_string()
    config['meta_data']['cycler_meta']['model'] = Values.TEST_HELPER.generate_random_string()
    loader = Loader(config)
    cycler_type_id = loader._Loader__lookup_cycler_type_id()
    assert (cycler_type_id is None)
    del loader


@pytest.mark.database
@pytest.mark.load
def test_lookup_cycler_id():

    config = deepcopy(Values.CONFIG_1)

    # Lookup existing cycler id
    loader = Loader(config)
    cycler_id = loader._lookup_cycler_id()
    assert (cycler_id == Values.TEST_HELPER.cycler_id)
    del loader

    # Lookup non-existing cycler with existing cycler_type_id
    config['meta_data']['cycler']['sn'] = Values.TEST_HELPER.generate_random_string()
    loader = Loader(config)
    cycler_id = loader._lookup_cycler_id()
    assert (cycler_id is None)
    del loader


@pytest.mark.database
@pytest.mark.load
def test_lookup_test_id():

    config = deepcopy(Values.CONFIG_1)

    # Lookup existing test id
    loader = Loader(config)
    test_id = loader._lookup_test_id()
    assert (test_id == Values.TEST_HELPER.test_id)
    del loader

    # Lookup non-existent test.
    config['meta_data']['test_meta']['test_name'] = Values.TEST_HELPER.generate_random_string()
    loader = Loader(config)
    test_id = loader._lookup_test_id()
    assert (test_id is None)
    del loader


@pytest.mark.database
@pytest.mark.load
def test_lookup_latest_unixtime():

    config = deepcopy(Values.CONFIG_1)

    Values.TEST_HELPER.delete_test_data()

    # Lookup unixtime_s for an existing test with no data in data table.
    loader = Loader(config)
    latest_unixtime_s = loader._Loader__lookup_latest_unixtime()
    assert (latest_unixtime_s is None)

    # Define data to upload
    data_dict = {
        'test_id': [Values.TEST_HELPER.test_id, Values.TEST_HELPER.test_id],
        'cycle': [1, 1],
        'step': [1, 1],
        'test_time_s': [1.0, 2.0],
        'step_time_s': [1.0, 2.0],
        'current_ma': [0.0, 0.0],
        'voltage_mv': [3.789, 3.800],
        'recorded_datetime': [pd.Timestamp(1674659265, unit='s', tz='US/Pacific'),
                              pd.Timestamp(1674659266, unit='s', tz='US/Pacific')],
        'unixtime_s': [1674659265.0, 1674659266.0]
    }
    df = pd.DataFrame(data=data_dict)

    # Load data to the table.
    num_rows_inserted = Values.TEST_HELPER.load_df_to_db(
        df=df, target_table='test_data')
    assert (num_rows_inserted == 2)

    # Check latest unixtims_s matches loaded data.
    latest_unixtime_s = loader._Loader__lookup_latest_unixtime()
    assert (latest_unixtime_s == data_dict['unixtime_s'][-1])

    del loader
    Values.TEST_HELPER.delete_test_data()

    # Lookup unixtime_s for a non-existent test with no data in data table.
    config['meta_data']['test_meta']['test_name'] = Values.TEST_HELPER.generate_random_string()
    loader = Loader(config)
    latest_unixtime_s = loader._Loader__lookup_latest_unixtime()
    assert (latest_unixtime_s is None)


@pytest.mark.database
@pytest.mark.load
def test_lookup_customer_id():

    config = deepcopy(Values.CONFIG_1)

    # Lookup existing customer id
    loader = Loader(config)
    customer_id = loader._Loader__lookup_customer_id()
    assert (customer_id == Values.TEST_HELPER.customer_id)
    del loader

    # Lookup non-existent customer.
    config['meta_data']['customers']['customer_name'] = Values.TEST_HELPER.generate_random_string()
    loader = Loader(config)
    customer_id = loader._Loader__lookup_customer_id()
    assert (customer_id is None)
    del loader


@pytest.mark.database
@pytest.mark.load
def test_lookup_project_id():

    config = deepcopy(Values.CONFIG_1)

    # Lookup existing project id
    loader = Loader(config)
    project_id = loader._Loader__lookup_project_id()
    assert (project_id == Values.TEST_HELPER.project_id)
    del loader

    # Lookup non-existent project.
    config['meta_data']['projects']['project_name'] = Values.TEST_HELPER.generate_random_string()
    loader = Loader(config)
    project_id = loader._Loader__lookup_project_id()
    assert (project_id is None)
    del loader


@pytest.mark.database
@pytest.mark.load
def test_lookup_latest_cycle():

    config = deepcopy(Values.CONFIG_1)

    Values.TEST_HELPER.delete_test_data()

    # Lookup latest cycle for a test that does not have data.
    loader = Loader(config)
    latest_cycle = loader._Loader__lookup_latest_cycle()
    assert (latest_cycle is None)

    # Define data to upload
    data_dict = {
        'test_id': [Values.TEST_HELPER.test_id, Values.TEST_HELPER.test_id],
        'cycle': [1, 2],
        'reported_charge_capacity_mah': [3000, 2999],
        'reported_charge_capacity_mah': [2995, 2994],
        'reported_coulombic_efficiency': [0.99, 0.98]
    }
    df = pd.DataFrame(data=data_dict)

    # Load data to the table.
    num_rows_inserted = Values.TEST_HELPER.load_df_to_db(
        df=df, target_table='test_data_cycle_stats')
    assert (num_rows_inserted == 2)

    # Check latest unixtime_s matches loaded data.
    latest_cycle = loader._Loader__lookup_latest_cycle()
    assert (latest_cycle == data_dict['cycle'][-1])

    Values.TEST_HELPER.delete_test_data()
    del loader

    # Lookup cycle for a test that does not exist.
    config['meta_data']['test_meta']['test_name'] = Values.TEST_HELPER.generate_random_string()
    loader = Loader(config)
    latest_cycle = loader._Loader__lookup_latest_cycle()
    assert (latest_cycle is None)


@pytest.mark.database
@pytest.mark.load
def test_load_test_data_small_dataset():

    config = deepcopy(Values.CONFIG_1)

    Values.TEST_HELPER.delete_test_data()

    # Define data to load.
    data_dict = {
        'cycle': [1, 1],
        'step': [1, 1],
        'test_time_s': [1.0, 2.0],
        'step_time_s': [1.0, 2.0],
        'current_ma': [0.0, 0.0],
        'voltage_mv': [3.789, 3.800],
        'recorded_datetime': [pd.Timestamp(1674659265, unit='s', tz='US/Pacific'),
                              pd.Timestamp(1674659266, unit='s', tz='US/Pacific')],
        'unixtime_s': [1674659265.0, 1674659266.0],
        'other_1': ['a', 'b'],
        'other_2': [1, 2]
    }
    df = pd.DataFrame(data=data_dict)

    # Load data
    loader = Loader(config)
    num_loaded_rows = loader.load_test_data(df)
    assert (num_loaded_rows == 2)

    # Pull loaded data and confirm it matches what we loaded.
    df_sql = pd.read_sql('SELECT * FROM test_data WHERE test_id = ' +
                         str(Values.TEST_HELPER.test_id) + ' ORDER BY unixtime_s;', Values.TEST_HELPER.engine)
    sql_dict = df_sql.to_dict(orient='list')
    # Convert the other_details column to a list of dicts.
    pd_other_details = [json.loads(row) for row in [
        '{"other_1": "a", "other_2": 1}',
        '{"other_1": "b", "other_2": 2}'
    ]]

    for key in data_dict.keys():
        if key == 'other_1' or key == 'other_2':
            # The other_1 and other_2 columns are stored as JSON strings.
            sq_dict_other_details = [row for row in sql_dict['other_details']]
            assert (sq_dict_other_details == pd_other_details)
        else:
            assert (data_dict[key] == sql_dict[key])

    # Try to re-load data that already exists in the database.
    num_loaded_rows = loader.load_test_data(df)
    assert (num_loaded_rows == 0)

    # Define more data to insert and load it
    new_data_dict = {
        'cycle': [1, 1],
        'step': [2, 2],
        'test_time_s': [3.0, 4.0],
        'step_time_s': [1.0, 2.0],
        'current_ma': [2.1, 2.1],
        'voltage_mv': [4.000, 4.020],
        'recorded_datetime': [pd.Timestamp(1674659267, unit='s', tz='US/Pacific'),
                              pd.Timestamp(1674659268, unit='s', tz='US/Pacific')],
        'unixtime_s': [1674659267.0, 1674659268.0]
    }
    df_new = pd.DataFrame(data=new_data_dict)
    num_loaded_rows = loader.load_test_data(df_new)
    assert (num_loaded_rows == 2)

    # Pull all loaded data and concat in to one dataset.
    df_sql = pd.read_sql('SELECT * FROM test_data WHERE test_id = ' +
                         str(Values.TEST_HELPER.test_id)+';', Values.TEST_HELPER.engine)
    sql_dict = df_sql.to_dict(orient='list')
    df = pd.concat([df, df_new])
    data_dict_combined = df.to_dict(orient='list')
    # Convert the other_details column to a list of dicts.
    pd_other_details_combined = [json.loads(row) if row else None for row in [
        '{"other_1": "a", "other_2": 1}',
        '{"other_1": "b", "other_2": 2}',
        None,
        None,
    ]]

    # Verify loaded data against all reference data.
    for key in data_dict_combined.keys():
        if key == 'other_1' or key == 'other_2':
            sq_dict_other_details = [
                row if row else None for row in sql_dict['other_details']]
            assert (pd_other_details_combined == pd_other_details_combined)
        else:
            assert (data_dict_combined[key] == sql_dict[key])

    # Delete the inserted data from database
    Values.TEST_HELPER.delete_test_data()


@pytest.mark.database
@pytest.mark.load
def test_load_cycle_stats_small_dataset():

    config = deepcopy(Values.CONFIG_1)

    Values.TEST_HELPER.delete_test_data()

    # Define data to load
    data_dict = {
        'cycle': [1, 2],
        'reported_charge_capacity_mah': [3000, 2999],
        'reported_discharge_capacity_mah': [2995, 2994],
        'reported_coulombic_efficiency': [0.99, 0.98]
    }
    df = pd.DataFrame(data=data_dict)

    # Load data
    loader = Loader(config)
    num_loaded_rows = loader.load_cycle_stats(df)
    assert (num_loaded_rows == 2)

    # Pull loaded data and verify it matches.
    df_sql = pd.read_sql(
        'SELECT * FROM test_data_cycle_stats WHERE test_id = '+str(Values.TEST_HELPER.test_id)+' ORDER BY cycle;', Values.TEST_HELPER.engine)
    sql_dict = df_sql.to_dict(orient='list')
    for key in data_dict.keys():
        assert (data_dict[key] == sql_dict[key])

    # Overwrite the existing data with slightly different data.
    data_dict = {
        'cycle': [1, 2],
        'reported_charge_capacity_mah': [3001, 2998],
        'reported_discharge_capacity_mah': [2996, 2995],
        'reported_coulombic_efficiency': [0.95, 0.96]
    }
    df = pd.DataFrame(data=data_dict)
    loader = Loader(config)
    num_loaded_rows = loader.load_cycle_stats(df)
    assert (num_loaded_rows == 2)

    # Pull loaded data and verify it matches.
    df_sql = pd.read_sql(
        'SELECT * FROM test_data_cycle_stats WHERE test_id = '+str(Values.TEST_HELPER.test_id)+' ORDER BY cycle;', Values.TEST_HELPER.engine)
    sql_dict = df_sql.to_dict(orient='list')
    for key in data_dict.keys():
        assert (data_dict[key] == sql_dict[key])

    # Define more data to insert and load it
    new_data_dict = {
        'cycle': [3, 4],
        'reported_charge_capacity_mah': [2998, 2997],
        'reported_discharge_capacity_mah': [2993, 2992],
        'reported_coulombic_efficiency': [0.97, 0.97]
    }
    df_new = pd.DataFrame(data=new_data_dict)
    num_loaded_rows = loader.load_cycle_stats(df_new)
    assert (num_loaded_rows == 2)

    # Pull all loaded data and verify
    df_sql = pd.read_sql(
        'SELECT * FROM test_data_cycle_stats WHERE test_id = '+str(Values.TEST_HELPER.test_id)+' ORDER BY cycle;', Values.TEST_HELPER.engine)
    sql_dict = df_sql.to_dict(orient='list')
    df = pd.concat([df, df_new])
    data_dict_combined = df.to_dict(orient='list')
    for key in data_dict_combined.keys():
        assert (data_dict_combined[key] == sql_dict[key])

    Values.TEST_HELPER.delete_test_data()


@pytest.mark.database
@pytest.mark.load
def test_insert_cycler_meta():

    config = deepcopy(Values.CONFIG_1)

    # Load new cycler meta
    config['meta_data']['cycler_meta']['model'] = Values.TEST_HELPER.generate_random_string()
    loader = Loader(config)
    cycler_type_id = loader._Loader__insert_cycler_meta()
    assert (cycler_type_id)

    # Make sure loaded data matches what is in config.
    new_row = Values.TEST_HELPER.read_last_row(
        target_table='cyclers_meta', pk_col_name='cycler_type_id')
    new_cycler_type_id = new_row[0]
    new_manufacturer = new_row[1]
    new_model = new_row[2]
    assert (new_cycler_type_id == cycler_type_id)
    assert (new_manufacturer ==
            config['meta_data']['cycler_meta']['manufacturer'])
    assert (new_model == config['meta_data']['cycler_meta']['model'])

    Values.TEST_HELPER.delete_entry(target_table='cyclers_meta',
                                    pk_col_name='cycler_type_id', pk_id=cycler_type_id)


@pytest.mark.database
@pytest.mark.load
def test_insert_cycler():

    config = deepcopy(Values.CONFIG_1)

    # Test inserting a cycler with a cycler_type_id that already exists.
    config['meta_data']['cycler']['sn'] = Values.TEST_HELPER.generate_random_string()
    loader = Loader(config)
    cycler_type_id = loader._Loader__lookup_cycler_type_id()
    assert (cycler_type_id)
    cycler_id = loader._insert_cycler()
    assert (cycler_id)

    # Make sure loaded data matches what is in config.
    new_row = Values.TEST_HELPER.read_last_row(
        target_table='cyclers', pk_col_name='cycler_id')
    newest_cycler_id = new_row[0]
    newest_cycler_type_id = new_row[1]
    newest_sn = new_row[2]
    assert (newest_cycler_id == cycler_id)
    assert (newest_cycler_type_id == cycler_type_id)
    assert (newest_sn == config['meta_data']['cycler']['sn'])

    del loader
    Values.TEST_HELPER.delete_entry(target_table='cyclers',
                                    pk_col_name='cycler_id', pk_id=cycler_id)

    # Test inserting a cycler with with a new cycler_type.
    config['meta_data']['cycler_meta']['manufacturer'] = Values.TEST_HELPER.generate_random_string()
    config['meta_data']['cycler_meta']['model'] = Values.TEST_HELPER.generate_random_string()
    config['meta_data']['cycler']['sn'] = Values.TEST_HELPER.generate_random_string()
    loader = Loader(config)
    cycler_type_id = loader._Loader__lookup_cycler_type_id()
    assert (cycler_type_id is None)
    cycler_id = loader._insert_cycler()
    assert (cycler_id)
    cycler_type_id = loader._Loader__lookup_cycler_type_id()
    assert (cycler_type_id)

    # Get the latest entry in the cyclers table.
    new_row = Values.TEST_HELPER.read_last_row(
        target_table='cyclers', pk_col_name='cycler_id')
    newest_cycler_id = new_row[0]
    newest_cycler_type_id = new_row[1]
    newest_sn = new_row[2]
    assert (newest_cycler_id == cycler_id)
    assert (newest_cycler_type_id == cycler_type_id)
    assert (newest_sn == config['meta_data']['cycler']['sn'])

    Values.TEST_HELPER.delete_entry(target_table='cyclers',
                                    pk_col_name='cycler_id', pk_id=cycler_id)
    Values.TEST_HELPER.delete_entry(target_table='cyclers_meta',
                                    pk_col_name='cycler_type_id', pk_id=cycler_type_id)


@pytest.mark.database
@pytest.mark.load
def test_insert_cell_meta():

    config = deepcopy(Values.CONFIG_1)

    # Insert non-existing cell_meta.
    config['meta_data']['cell_meta']['manufacturer'] = Values.TEST_HELPER.generate_random_string()
    config['meta_data']['cell_meta']['manufacturer_pn'] = Values.TEST_HELPER.generate_random_string()
    loader = Loader(config)
    cell_type_id = loader._Loader__lookup_cell_type_id()
    assert (cell_type_id is None)
    cell_type_id = loader._Loader__insert_cell_meta()
    assert (cell_type_id)

    new_row = Values.TEST_HELPER.read_last_row(
        target_table='cells_meta', pk_col_name='cell_type_id')
    newest_cell_type_id = new_row[0]
    newest_manufacturer = new_row[1]
    newest_manufacturer_pn = new_row[2]
    assert (newest_cell_type_id == cell_type_id)
    assert (newest_manufacturer ==
            config['meta_data']['cell_meta']['manufacturer'])
    assert (newest_manufacturer_pn ==
            config['meta_data']['cell_meta']['manufacturer_pn'])

    del loader
    Values.TEST_HELPER.delete_entry(target_table='cells_meta',
                                    pk_col_name='cell_type_id', pk_id=cell_type_id)


@pytest.mark.database
@pytest.mark.load
def test_insert_cell():

    config = deepcopy(Values.CONFIG_1)

    # Test inserting a cell with a cell_type_id that already exists.
    config['meta_data']['cell']['manufacturer_sn'] = Values.TEST_HELPER.generate_random_string()
    config['meta_data']['cell']['label'] = Values.TEST_HELPER.generate_random_string()
    loader = Loader(config)
    cell_type_id = loader._Loader__lookup_cell_type_id()
    assert (cell_type_id)
    cell_id = loader._Loader__insert_cell()
    assert (cell_id)

    new_row = Values.TEST_HELPER.read_last_row(
        target_table='cells', pk_col_name='cell_id')
    newest_cell_id = new_row[0]
    newest_cell_type_id = new_row[1]
    newest_sn = new_row[7]
    newest_label = new_row[3]
    assert (newest_cell_id == cell_id)
    assert (newest_cell_type_id == cell_type_id)
    assert (newest_sn == config['meta_data']['cell']['manufacturer_sn'])
    assert(newest_label == config['meta_data']['cell']['label'])

    del loader
    Values.TEST_HELPER.delete_entry(target_table='cells',
                                    pk_col_name='cell_id', pk_id=cell_id)

    # Test inserting a cycler with a cycler_type_id that does not exist.
    config['meta_data']['cell_meta']['manufacturer'] = Values.TEST_HELPER.generate_random_string()
    config['meta_data']['cell_meta']['manufacturer_pn'] = Values.TEST_HELPER.generate_random_string()
    config['meta_data']['cell']['manufacturer_sn'] = Values.TEST_HELPER.generate_random_string()
    loader = Loader(config)
    cell_type_id = loader._Loader__lookup_cell_type_id()
    assert (cell_type_id is None)
    cell_id = loader._Loader__insert_cell()
    assert (cell_id)
    cell_type_id = loader._Loader__lookup_cell_type_id()
    assert (cell_type_id)

    new_row = Values.TEST_HELPER.read_last_row(
        target_table='cells', pk_col_name='cell_id')
    newest_cell_id = new_row[0]
    newest_cell_type_id = new_row[1]
    newest_sn = new_row[7]
    assert (newest_cell_id == cell_id)
    assert (newest_cell_type_id == cell_type_id)
    assert (newest_sn == config['meta_data']['cell']['manufacturer_sn'])

    del loader
    Values.TEST_HELPER.delete_entry(target_table='cells',
                                    pk_col_name='cell_id', pk_id=cell_id)
    Values.TEST_HELPER.delete_entry(target_table='cells_meta',
                                    pk_col_name='cell_type_id', pk_id=cell_type_id)


@pytest.mark.database
@pytest.mark.load
def test_insert_schedule_meta():

    config = deepcopy(Values.CONFIG_1)

    # Insert a new schedule
    config['meta_data']['schedule_meta']['schedule_name'] = Values.TEST_HELPER.generate_random_string()
    loader = Loader(config)
    schedule_id = loader._Loader__lookup_schedule_id()
    assert (schedule_id is None)
    schedule_id = loader._Loader__insert_schedule_meta()
    assert (schedule_id)

    new_row = Values.TEST_HELPER.read_last_row(
        target_table='schedule_meta', pk_col_name='schedule_id')
    newest_schedule_id = new_row[0]
    newest_schedule_file_name = new_row[1]
    assert (newest_schedule_id == schedule_id)
    assert (newest_schedule_file_name ==
            config['meta_data']['schedule_meta']['schedule_name'])

    Values.TEST_HELPER.delete_entry(
        target_table='schedule_meta', pk_col_name='schedule_id', pk_id=schedule_id)


@pytest.mark.database
@pytest.mark.load
def test_insert_test_meta():

    config = deepcopy(Values.CONFIG_1)

    # Test inserting a test with a cell, schedule, and cycler that already exist
    config['meta_data']['test_meta']['test_name'] = Values.TEST_HELPER.generate_random_string()
    loader = Loader(config)
    cell_id = loader._lookup_cell_id()
    assert (cell_id)
    schedule_id = loader._Loader__lookup_schedule_id()
    assert (schedule_id)
    cycler_id = loader._lookup_cycler_id()
    assert (cycler_id)
    test_id = loader._Loader__insert_test_meta()
    assert (test_id)

    new_row = Values.TEST_HELPER.read_last_row(
        target_table='test_meta', pk_col_name='test_id')
    newest_test_id = new_row[0]
    newest_cell_id = new_row[1]
    newest_schedule_id = new_row[2]
    newest_test_name = new_row[3]
    newest_cycler_id = new_row[-1]
    assert (newest_test_id == test_id)
    assert (newest_cell_id == cell_id)
    assert (newest_cycler_id == cycler_id)
    assert (newest_schedule_id == schedule_id)
    assert (newest_test_name == config['meta_data']['test_meta']['test_name'])

    del loader
    Values.TEST_HELPER.delete_entry(target_table='test_meta',
                                    pk_col_name='test_id', pk_id=test_id)

    # Now insert a test with new everything
    config = deepcopy(Values.CONFIG_1)
    config['meta_data']['test_meta']['test_name'] = Values.TEST_HELPER.generate_random_string()
    config['meta_data']['schedule_meta']['schedule_name'] = Values.TEST_HELPER.generate_random_string()
    config['meta_data']['cell']['manufacturer_sn'] = Values.TEST_HELPER.generate_random_string()
    config['meta_data']['cell_meta']['manufacturer'] = Values.TEST_HELPER.generate_random_string()
    config['meta_data']['cell_meta']['manufacturer_pn'] = Values.TEST_HELPER.generate_random_string()
    config['meta_data']['cycler']['sn'] = Values.TEST_HELPER.generate_random_string()
    config['meta_data']['cycler_meta']['manufacturer'] = Values.TEST_HELPER.generate_random_string()
    config['meta_data']['cycler_meta']['model'] = Values.TEST_HELPER.generate_random_string()
    loader = Loader(config)

    # Make sure no fields existed before.
    assert (loader._lookup_cell_id() is None)
    assert (loader._Loader__lookup_cell_type_id() is None)
    assert (loader._Loader__lookup_schedule_id() is None)
    assert (loader._lookup_cycler_id() is None)
    assert (loader._Loader__lookup_cycler_type_id() is None)

    # Insert new test data
    test_id = loader._Loader__insert_test_meta()
    assert (test_id)

    # Get the IDs for the newly inserted data
    schedule_id = loader._Loader__lookup_schedule_id()
    assert (schedule_id)
    cycler_id = loader._lookup_cycler_id()
    assert (cycler_id)
    cycler_type_id = loader._Loader__lookup_cycler_type_id()
    assert (cycler_type_id)
    cell_id = loader._lookup_cell_id()
    assert (cell_id)
    cell_type_id = loader._Loader__lookup_cell_type_id()
    assert (cell_type_id)

    # Make sure all uploaded data matches
    new_row = Values.TEST_HELPER.read_last_row(
        target_table='test_meta', pk_col_name='test_id')
    newest_test_id = new_row[0]
    newest_cell_id = new_row[1]
    newest_schedule_id = new_row[2]
    newest_test_name = new_row[3]
    newest_cycler_id = new_row[-1]
    assert (newest_test_id == test_id)
    assert (newest_cell_id == cell_id)
    assert (newest_cycler_id == cycler_id)
    assert (newest_schedule_id == schedule_id)
    assert (newest_test_name == config['meta_data']['test_meta']['test_name'])

    del loader
    Values.TEST_HELPER.delete_entry(
        target_table='test_meta', pk_col_name='test_id', pk_id=test_id)
    Values.TEST_HELPER.delete_entry(
        target_table='cyclers', pk_col_name='cycler_id', pk_id=cycler_id)
    Values.TEST_HELPER.delete_entry(
        target_table='cyclers_meta', pk_col_name='cycler_type_id', pk_id=cycler_type_id)
    Values.TEST_HELPER.delete_entry(
        target_table='cells', pk_col_name='cell_id', pk_id=cell_id)
    Values.TEST_HELPER.delete_entry(
        target_table='cells_meta', pk_col_name='cell_type_id', pk_id=cell_type_id)
    Values.TEST_HELPER.delete_entry(
        target_table='schedule_meta', pk_col_name='schedule_id', pk_id=schedule_id)
