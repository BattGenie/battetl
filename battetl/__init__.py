from .constants import Constants
from .logger import logger
from .utils import Utils
from .BattETL import BattETL
from .battetl_quick import battetl_quick
import argparse
import os
import re
import json

def run_battetl():
    """
    Run the BattETL application with command-line interface.

    This function parses command-line arguments and executes appropriate actions based on the provided commands.

    Command-Line Arguments:
    -c, --config: Configuration command. If specified, it creates a new configuration file.
    -e, --extract: Extract command. If specified, it triggers the data extraction process.
    -t, --transform: Transform command. If specified, it triggers the data transformation process.
    -l, --load: Load command. If specified, it triggers the data loading process.
    -etl, --etl: ETL command. If specified, it triggers the full ETL (Extract, Transform, Load) process.

    Optional Argument:
    config_file_path: Path to the configuration file. If provided, it overrides the default configuration file path.
                      If not provided, a default configuration file named 'demo_config.json' is used.

    Returns:
    None

    Raises:
    None

    Example Usage:
    To create a new configuration file:
        python script.py --config /path/to/config.json

    To run a specific step of the ETL process:
        python script.py --extract
        python script.py --transform
        python script.py --load

    To run the full ETL process:
        python script.py --etl

    To run the BattETL in quick mode with default configuration:
        python script.py
    """
    parser = argparse.ArgumentParser(description="BattETL Command Line Interface")
    parser.add_argument('-c', "--config", action='store_true', help='Configuration command')
    parser.add_argument('-e', "--extract", action='store_true', help='Configuration command')
    parser.add_argument('-t', "--transform", action='store_true', help='Configuration command')
    parser.add_argument('-l', "--load", action='store_true', help='Configuration command')
    parser.add_argument('-etl', "--etl", action='store_true', help='Configuration command')
    parser.add_argument('config_file_path', nargs='?', type=str, help='Path to the file (optional)')
    args = parser.parse_args()
    if args.config:
        if not args.config_file_path:
            print("Please specify file path")
            return
        file_path = os.path.join(os.getcwd(), args.config_file_path)
        create_config(file_path)
    else:
        file_path = args.config_file_path or 'demo_config.json'
        file_path = os.path.join(os.getcwd(), file_path)
        cell = BattETL(file_path)
        if args.extract:
            cell.extract()
        elif args.transform:
            cell.transform()
        elif args.load:
            cell.load()
        elif args.etl:
            cell.extract().transform().load()
        else:
            print("Running BattETL in quick mode....")


def create_config(data_folder_path):
    """
    Create a configuration file based on the contents of the specified data folder.

    This function scans the provided data folder for relevant files and generates a configuration
    file (in JSON format) with metadata about the files and the test setup.

    Arguments:
    data_folder_path (str): The path to the data folder containing the test files.

    Returns:
    None

    Example Usage:
    create_config('/path/to/data/folder')

    File Naming Conventions:
    - For Maccor data files: Files ending with a number followed by '.txt'.
    - For Maccor stats files: Files ending with '[STATS].txt'.
    - For Maccor schedule files: Files ending with '.000'.
    - For Arbin data files: Files containing 'Wb' in the name and ending with '.CSV'.
    - For Arbin stats files: Files ending with 'StatisticByCycle.CSV'.
    - For Maccor schedule files: Files ending with '.sdx'.

    Generated Configuration Structure:
    The configuration file includes metadata about the test setup, including timezone, file paths,
    and various parameters related to the test, cell, schedule, cycler, customers, and projects.

    The configuration structure is as follows:
    {
        "timezone": "America/Los_Angeles",
        "data_file_path": data_files,
        "stats_file_path": stats_files,
        "schedule_file_path": schedule_files,
        "meta_data": {
            "test_meta": {
                "cell_id": None,
                "schedule_id": None,
                "test_name": os.path.basename(data_files[0]).split(' ', 1)[0],
                "start_date": '2020-10-06',
                "end_date": '2020-10-11',
                "channel": int(os.path.basename(data_files[0]).split('.', 1)[0].split(' ')[-1]),
                "ev_chamber": 12,
                "ev_chamber_slot": None,
                "thermocouples": None,
                "thermocouple_channels": None,
                "comments": "Ran at 45 degrees C",
                "project_id": None,
                "test_capacity_mah": 2650,
                "potentiostat_id": None,
                "cycler_id": None,
            },
            "cell": {
                "cell_type_id": None,
                "batch_number": "BATCH_NUMBER",
                "label": "24",
                "date_received": "2020-09-01",
                "comments": None,
                "date_manufactured": None,
                "manufacturer_sn": "BattGenie_SN",
                "dims": None,
                "weight_g": None,
                "first_received_at_voltage_mv": None,
            },
            "cell_meta": {
                "manufacturer": "BattGenie",
                "manufacturer_pn": "BattGenie_PN",
                "form_factor": "pouch",
                "capacity_mah": 2720,
                "chemistry": None,
                "dimensions": '{"x_mm":"54.25", "y_mm":106.96, "z_mm":3.19}',
                "datasheet": None,
            },
            "schedule_meta": {
                "schedule_name": "BG_Characterization_v1",
                "test_type": "Characterization",
                "cycler_make": "Maccor",
                "date_created": "2020-10-06",
                "created_by": "BattGenie",
                "comments": None,
                "cv_voltage_threshold_mv": None,
                "details": None,
            },
            "cycler": {
                "sn": "SN",
                "calibration_date": None,
                "calibration_due_date": None,
                "location": "BattGenie",
                "timezone_based": None,
            },
            "cycler_meta": {
                "manufacturer": "Maccor",
                "model": "SERIES 4000M",
                "datasheet": None,
                "num_channels": None,
                "lower_current_limit_a": None,
                "upper_current_limit_a": None,
                "lower_voltage_limit_v": None,
                "upper_voltage_limit_v": None,
            },
            "customers": {
                "customer_name": "FakeCustomer"
            },
            "projects": {
                "project_name": "FakeProject"
            }
        }
    }
    """
    battetl_dir = os.getcwd()
    data_directory = os.path.join(battetl_dir, data_folder_path)
    DATA_FOLDER = os.path.abspath(data_directory)
    print(f'Check data folder "{DATA_FOLDER}"')

    data_files = []
    stats_files = []
    schedule_files = []
    # list files in data folder
    for file in os.listdir(DATA_FOLDER):
        # Maccor: Check if file name ends with number.txt
        if re.search(r'\d+\.txt$', file):
            data_files.append(os.path.join(DATA_FOLDER, file))

        # Maccor: Check if file name ends with [STATS].txt
        elif re.search(r'\[STATS\]\.txt$', file):
            stats_files.append(os.path.join(DATA_FOLDER, file))

        # Maccor: Check if file name ends with .000
        elif re.search(r'\.000$', file):
            schedule_files.append(os.path.join(DATA_FOLDER, file))

        # Check: Check if Arbin data file
        elif ('Wb' in re.split('_', file) and re.split('_', file)[-1].endswith('.CSV')):
            data_files.append(os.path.join(DATA_FOLDER, file))

        # Arbin : Stats file
        elif re.search(r'StatisticByCycle.CSV$', file):
            stats_files.append(os.path.join(DATA_FOLDER, file))

        # Maccor: Check if file schedule file
        elif re.search(r'.sdx$', file):
            schedule_files.append(os.path.join(DATA_FOLDER, file))

    # Sort files by name, ascending
    data_files.sort(reverse=False)
    stats_files.sort(reverse=False)

    print(f'>> Found {len(data_files)} data files.')
    print('\n'.join(data_files))
    print(f'>> Found {len(stats_files)} stats files.')
    print('\n'.join(stats_files))
    print(f'>> Found {len(schedule_files)} schedule files.')
    print('\n'.join(schedule_files))

    config = {
        "timezone": "America/Los_Angeles",
        "data_file_path": data_files,
        "stats_file_path": stats_files,
        "schedule_file_path": schedule_files,
        "meta_data": {
            "test_meta": {
                "cell_id": None,
                "schedule_id": None,
                # Get test name from first data file. The name is before first space.
                "test_name": os.path.basename(data_files[0]).split(' ', 1)[0],
                "start_date": '2020-10-06',
                "end_date": '2020-10-11',
                # Get the channel number from the first data file. The number is before the first dot.
                "channel": int(os.path.basename(data_files[0]).split('.', 1)[0].split(' ')[-1]),
                "ev_chamber": 12,
                "ev_chamber_slot": None,
                "thermocouples": None,
                "thermocouple_channels": None,
                "comments": "Ran at 45 degrees C",
                "project_id": None,
                "test_capacity_mah": 2650,
                "potentiostat_id": None,
                "cycler_id": None,
            },
            "cell": {
                "cell_type_id": None,
                "batch_number": "BATCH_NUMBER",
                "label": "24",
                "date_received": "2020-09-01",
                "comments": None,
                "date_manufactured": None,
                "manufacturer_sn": "BattGenie_SN",
                "dims": None,
                "weight_g": None,
                "first_received_at_voltage_mv": None,
            },
            "cell_meta": {
                "manufacturer": "BattGenie",
                "manufacturer_pn": "BattGenie_PN",
                "form_factor": "pouch",
                "capacity_mah": 2720,
                "chemistry": None,
                "dimensions": '{"x_mm":"54.25", "y_mm":106.96, "z_mm":3.19}',
                "datasheet": None,
            },
            "schedule_meta": {
                "schedule_name": "BG_Characterization_v1",
                "test_type": "Characterization",
                "cycler_make": "Maccor",
                "date_created": "2020-10-06",
                "created_by": "BattGenie",
                "comments": None,
                "cv_voltage_threshold_mv": None,
                "details": None,
            },
            "cycler": {
                "sn": "SN",
                "calibration_date": None,
                "calibration_due_date": None,
                "location": "BattGenie",
                "timezone_based": None,
            },
            "cycler_meta": {
                "manufacturer": "Maccor",
                "model": "SERIES 4000M",
                "datasheet": None,
                "num_channels": None,
                "lower_current_limit_a": None,
                "upper_current_limit_a": None,
                "lower_voltage_limit_v": None,
                "upper_voltage_limit_v": None,
            },
            "customers": {
                "customer_name": "FakeCustomer"
            },
            "projects": {
                "project_name": "FakeProject"
            }
        }
    }

    # Deep remove None values from config
    config = {k: v for k, v in config.items() if v is not None}
    config['meta_data'] = {k: v for k, v in config['meta_data'].items() if v is not None}
    config['meta_data']['test_meta'] = {k: v for k, v in config['meta_data']['test_meta'].items() if v is not None}
    config['meta_data']['cell'] = {k: v for k, v in config['meta_data']['cell'].items() if v is not None}
    config['meta_data']['cell_meta'] = {k: v for k, v in config['meta_data']['cell_meta'].items() if v is not None}
    config['meta_data']['schedule_meta'] = {k: v for k, v in config['meta_data']['schedule_meta'].items() if
                                            v is not None}
    config['meta_data']['cycler'] = {k: v for k, v in config['meta_data']['cycler'].items() if v is not None}
    config['meta_data']['cycler_meta'] = {k: v for k, v in config['meta_data']['cycler_meta'].items() if v is not None}

    # Save config to file
    with open('demo_config.json', 'w') as f:
        json.dump(config, f, indent=4)
