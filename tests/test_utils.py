import os
import pytest
import pandas as pd
from pathlib import Path

from battetl import Utils

CONFIG_DIR = Path(__file__).parent / 'configs'
ENV_PATH = Path(__file__).parent / '.env.example'


@pytest.mark.utils
def test_utils_convert_datetime():
    # Create a DataFrame
    # 0	2023-01-01 00:00:00
    # 1	2023-01-01 01:00:00
    # 2	2023-01-01 02:00:00
    df = pd.DataFrame({'datetime': pd.date_range(
        "2023-01-01", periods=3, freq="H")})

    # UTC-08:00
    df_1 = Utils.convert_datetime(df.copy(), 'datetime', 'America/Los_Angeles')
    dt_1 = df_1.iloc[0]['datetime'].strftime('%Y-%m-%d %H:%M:%S')

    # UTC-07:00
    df_2 = Utils.convert_datetime(df.copy(), 'datetime', 'America/Phoenix')
    dt_2 = df_2.iloc[0]['datetime'].strftime('%Y-%m-%d %H:%M:%S')

    # UTC+01:00
    df_3 = Utils.convert_datetime(df.copy(), 'datetime', 'Europe/Berlin')
    dt_3 = df_3.iloc[0]['datetime'].strftime('%Y-%m-%d %H:%M:%S')

    # UTC+00:00 to UTC-08:00
    assert (dt_1 == '2023-01-01 08:00:00'), 'UTC-08:00. Should be 2023-01-01 08:00:00'

    # UTC+00:00 to UTC-07:00
    assert (dt_2 == '2023-01-01 07:00:00'), 'UTC-07:00. Should be 2023-01-01 07:00:00'

    # UTC+00:00 to UTC+01:00
    assert (dt_3 == '2022-12-31 23:00:00'), 'UTC+01:00. Should be 2022-12-31 23:00:00'


@pytest.mark.utils
def test_utils_load_env(env_example):
    Utils.load_env(ENV_PATH)
    credentials = {
        'ENV': os.getenv('ENV'),
        'DB_HOSTNAME': os.getenv('DB_HOSTNAME'),
        'DB_PORT': os.getenv('DB_PORT'),
        'DB_TARGET': os.getenv('DB_TARGET'),
        'DB_USERNAME': os.getenv('DB_USERNAME'),
        'DB_PASSWORD': os.getenv('DB_PASSWORD'),
    }
    assert credentials == env_example, 'Env not loaded correctly'


@pytest.mark.utils
class TestUtilsLoadConfig:
    def test_utils_load_json_config(self, cell_config):
        config_path = Path(CONFIG_DIR) / 'config_1.json'

        config = Utils.load_config(config_path)
        assert config == cell_config, 'JSON config not loaded correctly'

    def test_utils_load_yaml_config(self, cell_config):
        config_path = Path(CONFIG_DIR) / 'config_1.yaml'

        config = Utils.load_config(config_path)
        assert config == cell_config, 'YAML config not loaded correctly'

    def test_utils_load_invalid_config_path(self):
        config_path = Path(CONFIG_DIR) / 'invalid_path'

        with pytest.raises(FileNotFoundError):
            config = Utils.load_config(config_path)


@pytest.mark.utils
def test_utils_drop_empty_rows():
    test_df = pd.DataFrame({
        'A': [1.0, 2.0, None],
        'B': [None, None, None],
        'C': ['a', 'b', None],
    })
    assert len(test_df) == 3, 'Test DataFrame should have 3 rows'
    new_df = Utils.drop_empty_rows(test_df)
    assert len(new_df) == 2, 'New DataFrame should have 2 rows'
    assert new_df.equals(pd.DataFrame({
        'A': [1.0, 2.0],
        'B': [None, None],
        'C': ['a', 'b'],
    })), 'New DataFrame should equal expected DataFrame'
