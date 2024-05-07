import pytest
import os
from battetl import run_battetl, create_config
import sys


@pytest.fixture
def config_file_path(request):
    return request.config.getoption("--config_file_path")


@pytest.mark.cli
def test_config():
    try:
        script_directory = os.path.dirname(os.path.abspath(__file__))
        data_directory = os.path.join(script_directory, "data/maccor_cycler_data/simple_data")
        sys.argv = ["test_battetl_config", "-c", data_directory]
        run_battetl()
        file_path = os.path.join(os.getcwd(), 'demo_config.json')
        assert os.path.exists(file_path)
    except Exception as e:
        pytest.fail(f"Config creation raised an exception: {e}")


@pytest.mark.cli
def test_extract():
    try:
        sys.argv = ["test_battetl_config", "-e", "demo_config.json"]
        run_battetl()
    except Exception as e:
        pytest.fail(f"BattETL Extract raised an exception: {e}")


@pytest.mark.cli
def test_transform():
    try:
        sys.argv = ["test_battetl_config", "-t", "demo_config.json"]
        run_battetl()
    except Exception as e:
        pytest.fail(f"BattETL transform raised an exception: {e}")


@pytest.mark.cli
def test_load():
    try:
        sys.argv = ["test_battetl_config", "-l", "demo_config.json"]
        cell = run_battetl()
    except Exception as e:
        pytest.fail(f"BattETL Extract raised an exception: {e}")
