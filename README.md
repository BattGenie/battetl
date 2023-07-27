<img src="https://raw.githubusercontent.com/BattGenie/battetl/main/images/BattETL_Logo.png">

# BattETL

BattETL is a well-tested and an enterprise-ready python module for **E**xtracting, **T**ransforming, and **L**oading battery cycler data to a [database](https://github.com/BattGenie/battdb). BattETL can also be used just for data extraction and transformation if a database is not desired. Currently data from Maccor and Arbin cyclers are supported.

<img src="https://raw.githubusercontent.com/BattGenie/battetl/main/images/BattETL_Simple_System.svg">

## Overview

- [Motivation](#motivation)
- [Video Guides](#video-guides)
- [Installation](#installation)
  - [Requirements](#requirements)
  - [Installation Instructions](#installation-instructions)
- [Usage](#usage)
  - [Quick Mode](#quick-mode)
    - [Unstructured data](#unstructured-data)
  - [Config File](#config-file)
  - [Env File](#env-file)
  - [Data Export Requirements](#data-export-requirements)
    - [Maccor](#maccor)
    - [Arbin](#arbin)
- [BattETL Process Overview](#battetl-process-overview)
  - [System Diagram](#system-diagram)
  - [Transformer](#transformer)
  - [Extractor](#extractor)
  - [Loader](#loader)
- [Testing](#testing)
- [Troubleshooting](#troubleshooting)

## Motivation

### Why another battery cycler data ingesting tool?

There are published [open](https://github.com/TRI-AMDD/beep) [source](https://github.com/Battery-Intelligence-Lab/galvanalyser) solutions for battery cycler data ingestion. BattETL hopes to build on these tools and adds a few conveniences to support robust data management and repeatable data analytics.

### Some features of BattETL

- **A relational data destination.**

BattETL package is tightly coupled to the destination database it sends the data to. BattETL requires that for the battery data that is loaded to the database - the detailed metadata such as battery make and model, cycler make and model, and schedule/procedure information be also included. While these constraints (somewhat) slowdown the data ingestion process, it ensures verifiable, high-quality data persistance.

- **A database that can hold *all* relevant battery test information.**

Not only does the BattETL extract and store all the battery test data, it also captures the cycler schedule/procedure file. In addition to the schedule/procedure file, BattETL captures and stores all files associated with the schedule/procedure. This includes any current profiles (Maccor FastWave or Arbin current simulation files), EIS specifications, or even entire system specifications (Arbin batch files.)

- **Analytics built into the transformation step.**

As part of the transformation step BattETL calculates various cycle statistics (capacity, coulombic efficiency, CC/CV charge times, etc.) providing independent and supplemental cycle statistics to compliment those generated by the cycler software. These can particularly handy when easy-to-miss settings were overlooked in writing the schedule (e.g. forgetting to reset the capacity when incriminating the cycle.) Do you have some sort of hyper-boutique transformation or statistic calculation needed for a specific test? No problem. BattETL can apply user defined transformation functions too; all before the data is loaded to the database.

## Video Guides

The following are video guides BattETL and BattDB:

- [Installation & setup](https://www.youtube.com/watch?v=tmudLhrj9xE)
- [Design overview](https://www.youtube.com/watch?v=Y2tXInbkYF8)
- [Demonstration](https://www.youtube.com/watch?v=3wNd3fhqBwo)

## Installation

A video showing how to setup and install BattETL and BattDB can be found [here](https://www.youtube.com/watch?v=tmudLhrj9xE)

### Requirements

#### Software Requirements

- Python 3.9 or 3.10
- The required packages are listed in the `requirements.txt` file

#### Hardware Requirements

Based on processing approximately 10,000 rows and 12 columns with Pandas, BattETL requires a minimum of 13MB of RAM.

- RAM: 2 GB or higher.
- Disk Space: At least 2 GB of free space is recommended to accommodate the transformed data, as well as additional space for installation and storing data. The exact amount of disk space required will depend on the size of the data being processed and the resulting intermediate results.

### Installation Instructions

- Install BattETL using pip:

```sh
pip install battetl
```

- Install BattETL from source code:

```sh
git clone https://github.com/BattGenie/battetl.git
cd battetl
pip install -r requirements.txt
pip install .
```

> Note that if you wish to use the Load feature of BattETL it is necessary to deploy an instance of a [BattDB](https://github.com/BattGenie/battdb) database to load the data to. Follow the instructions there for deploying the database.

## Usage

A video demonstrating how to use BattETL and BattDB can be found [here](https://www.youtube.com/watch?v=3wNd3fhqBwo)

Using BattETL as an all-in-one ETL function is as easy as:

```python
from battetl import BattETL

cell = BattETL(config_path).extract().transform().load()
```

Where `config_path` is the absolute or relative path to BattETL [config file](#config-file).

It is also necessary to include an [.env file](#env-file) within the working directory that contains the associated database credentials.

Finally, the data must be exported from the cycler in a [specific format](#data-export-requirements)

For a practical implementation of `BattETL`, please refer to the example notebook `examples/battetl_demo.ipynb` located in the `examples` directory. This notebook demonstrates how to implement `BattETL` and can serve as a useful reference for your own project.

> Note that repeated calls to load test data that already exists in the database (as determined by unix timestamp) will not overwrite any existing data. Only test data after the most recent existing unix timestamp will be loaded. In the case of loading cycle statistics, any duplicate cycles that already exist in the database will be overwritten.

### Quick Mode

BattETL provides a Quick Mode, which automatically detects whether the given file is test data or test stats from a Maccor or an Arbin cycler, and uploads it to the database accordingly. Here's an example command line:

```bash
python -m battetl.battetl_quick file="TEST_DATA.txt" db_url="postgres://postgres:password@localhost:5454/battdb_quick"
```

This command relies on [BattDB](https://github.com/BattGenie/BattDB). If BattDB does not exist, a database will be created using Docker Compose and the data will be uploaded to it. The command also returns a harmonized Pandas dataframe that can be used for analysis and visualization. 

#### Unstructured data

Quick mode also supports importing battery test data not generated by an Arbin or Maccor cycler, henceforth referred to as "unstructured data". When handling unstructured data it is necessary to include a third command line argument, `file_meta.json` that will be used to define how the data should be imported and to rename column to conform to names expected by the database. An example file_meta is given below:

```JSON
{
    "pandas_read_csv_args" : {
        "delimiter":",",
        "index_col": false
    }, 
    "voltage_mv" : 
    {
        "column_name":"volt",
        "scaling_factor":1
    },
    "current_ma" : 
    {
        "column_name":"curr",
        "scaling_factor":1
    },
    "test_time_s" :
    {
        "column_name":"time",
        "scaling_factor":1
    },
    "cycle" :
    {
        "column_name":"cycl"
    }
}
```

The `pandas_read_csv_args` object holds any function arguments that should be passed to [`pandas.read_csv()`](https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html) to correctly import the data as a pandas dataframe.

All the the other objects have the following format:

```JSON
{
    "new_column_name" : 
    {
        "column_name" : "existing_column_name",
        "scaling_factor": 1
    }
}
```

Where:

- `new_column_name` is the new name of the column that should be set in the dataframe, e.g. `voltage_mv`
- `existing_column_name` is the existing column name that should be changed to the `new_column_name`
- `scaling_factor` *optional* provides an optional multiplicative scaling to use to scale the column value ot the desired units. 

A few notes:

- Column decoders must be included for `voltage_mv`, `current_ma`, and `test_time_s` or `recorded_datetime` so as to match the database schema. 
- Any column can be transformed, even it is to a name that does not exist in the database schema.
- Any column name does that does not exist in the `test_data` table will be loaded in the `other_details` column

So, the command to run unstructured data would like the following:

```bash
python -m battetl.battetl_quick file="TEST_DATA.txt" file_meta="file_meta.json" db_url="postgres://postgres:password@localhost:5454/battdb_quick"
```

### Config File

To use BattETL it is necessary to provide a path to JSON configuration file. This config file contains paths to the relevant test data files and test metadata used for analysis and establishing database relations. An example configuration file is given within `examples/battetl_config_example.json`

```json
{
    "timezone": "America/Los_Angeles",
    "data_file_path": [
        "abs/path/to/your/arbin/or/maccor/test/data/file(s).txt"
    ],
    "stats_file_path": [
        "abs/path/to/your/arbin/or/maccor/test/stats/file.txt"
    ],
    "schedule_file_path": [
        "abs/path/to/your/schedule/or/procedure/file(s).000"
    ],
    "meta_data": {
        "test_meta": {
            "test_name": "the_name_of_your_test",
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
        },
        "customers": {
            "customer_name": "FakeCustomer"
        },
        "projects": {
            "project_name": "FakeProject"
        }
    }
}
```

#### Cell Thermocouple (optional)

If the cell has a thermocouple, it is necessary to include the following in the header of the config file:

```json
"cell_thermocouple": 1
```

where `1` corresponds to the thermocouple index in the cycler test data export, i.e `Temp 1` for Maccor and `Aux_Temperature_1 (C)` for Arbin.

If no cell thermocouple value is listed, `calculated_max_charge_temp_c` and `calculated_max_discharge_temp_c` will be set to `null` in the `test_data_cycle_stats` table.

### Env File

The .env contains the associated database credentials and is formatted as follows

```text
ENV=dev # dev, prod
DB_TARGET=YOUR_DATABASE_NAME
DB_USERNAME=YOUR_USERNAME
DB_PASSWORD=YOUR_PASSWORD
DB_HOSTNAME=localhost
DB_PORT=5432
```

An example .env file is given within `examples/.env.example`

### Data Export Requirements

- [For Maccor Cycler](#maccor)
- [For Arbin Cycler](#arbin)

#### Maccor

From the "Maccor Export" tool data should be exported as "Text Output" in a "Tab Delimited" format. The output sheets should include "Data Records" and "Cycle Statistics". The box for "Discharge Current Exported Negative" should be checked.

The following columns are the minimum required for export:

For Data Records:

- Cycle : Header Label of Cyc#
- Step : Header Label of Step
- Test Time : Header Label of TestTime(s), Units of seconds
- Step Time : Header Label of StepTime(s), Units of seconds
- Capacity : Header Label of Capacity(Ah), Units of Amp-Hour
- Energy : Header Label of Watt-hr, Units of Watt-hour
- Current : Header Label of Current(A), Units of Amps
- Voltage : Header Label of Voltage(V), Units of Volts
- DPT Time : Header label of DPt Time

<img src="https://raw.githubusercontent.com/BattGenie/battetl/main/images/MaccorExport_DataRecords.png" width="85%" height="50%">

For Cycle Statistics:

- Cycle : Header Label of Cycle
- Capacity Chg : Header label AH-IN, Units of Ahr
- Capacity Dis : Header label AH-Out, Units of Ahr
- DPT Time : Header Label of DPt Time

<img src="https://raw.githubusercontent.com/BattGenie/battetl/main/images/MaccorExport_CycleStats.png" width="85%" height="50%">

#### Arbin

Within the Arbin Export tool the "File Type" should be set to "To CSV". Within "Data Filter" the box "statistics by Cycle" should be checked along with any other data. For range filter "All" should be selected.

<img src="https://raw.githubusercontent.com/BattGenie/battetl/main/images/ArbinExport.jpg" width="45%" height="25%">

An example notebook `battetl_demo.ipynb` that provides a tool to help generate config files is located in the `examples` directory.

## BattETL Overview

A video discussing the design of BattDB and BattETL can be found [here](https://www.youtube.com/watch?v=Y2tXInbkYF8)

BattETL bundles together the three independent submodules:

- [Extractor](#extractor) : Responsible for extractor the cycler data and schedule/procedure files.
- [Transformer](#transformer) : Responsible for transforming cycler data
- [Loader](#loader) : Responsible for loading data and associated data to the database.

After running BattETL on a specific configuration file the following objects will be available as class variables from the instance:

- `raw_test_data: pandas.DataFrame`: The raw test data exactly as it is in the generated cycler data file(s).
- `raw_cycle_stats: pandas.DataFrame`: The raw cycle stats data exactly as it is in the generated cycler statistics file.
- `test_data: pandas.DataFrame`: The transformed test data after normalizing units and datatype and adding a unixtime column.
- `cycle_stats: pandas.DataFrame`: The transformed cycle stats after normalizing units, datatypes, and calculating supplemental cycle statistics.
- `schedule: dict`: The procedure/schedule file and all associated files in a nested Dictionary.

### System diagram

<img src="https://raw.githubusercontent.com/BattGenie/battetl/main/images/BattETL_SystemDiagram.svg">

### Extractor

The Extractor is an interface that allows you to extract battery test data from raw data files. You can extract test data and cycle stats data from multiple files using `data_from_files`, and extract Arbin schedules or Maccor procedures and associated files using `schedule_from_files`.

For an example of the Extractor as a standalone class, see `examples/submodule_demos/Extractor_demo.ipynb`

#### Functions

- `data_from_files(paths: list[str])`: Extracts multiple test data files into a single pandas DataFrame.  
- `schedule_from_files(paths: list[str])`: Extracts Arbin schedules or Maccor procedures and associated files and stores them in a dictionary.  
- `from_pickle(path: str)`: Reads data from the passed file path and returns it as a pandas DataFrame.  

#### Variables

- `cycler_make: str`: Cycler make  
- `raw_test_data_meta_data: list[dict]`: Test data meta data  
- `raw_cycle_stats_meta_data: list[dict]`: Cycler stats meta data  
- `raw_test_data: pandas.DataFrame`: Test data  
- `raw_cycle_stats: pandas.DataFrame`: Cycler stats  
- `maccor_procedure: dict`: Maccor procedure  
- `arbin_schedule: dict`: Arbin schedule  

### Transformer

The Transformer is an interface that allows you to transform battery test data to the BattETL schema. By default, the time zone is set to 'America/Los_Angeles', but you can modify it to your preferred time zone using the names found in the [IANA Time Zone Database](https://www.iana.org/time-zones). In addition to the default functions provided by the Transformer, you can add your own custom functions to perform specific transformations on your data.

For an example of the Extractor as a standalone class, see `examples/submodule_demos/Transformer_demo.ipynb`

#### Functions

- `transform_test_data(self, data: pd.DataFrame)`: Transforms test data to conform to BattETL naming and data conventions  
- `transform_cycle_stats`: Transforms cycle stats to conform to BattETL naming and data conventions  

#### Variables

- `timezone: str`: Time zone strings in the IANA Time Zone Database. Used to convert to unix timestamp in seconds. Default 'America/Los_Angeles'.  
- `user_transform_test_data`: A user defined function to transform test data. The function should take a pandas.DataFrame as input and return a pandas.DataFrame as output.  
- `user_transform_cycle_stats`: A user defined function to transform cycle stats. The function should take a pandas.DataFrame as input and return a pandas.DataFrame as output.  
- `test_data: pandas.DataFrame`: Transformed test data  
- `cycle_stats: pandas.DataFrame`: Transformed cycle stats  

### Loader

The `load` module provides an interface to load the extracted data into a database.

Note that repeated calls to load test data that already exists in the database (as determined by unix timestamp) will not overwrite any existing data. Only test data after the latest existing unix timestamp will be loaded. In the case of loading cycle statistics, any duplicate cycles that already exist in the database will be overwritten.

For an example of the Extractor as a standalone class, see `examples/submodule_demos/Loader_demo.ipynb`

#### Functions

- `load_test_data(test_data_df)`: Loads test_data_df to `test_data` table in the specified database.  
- `load_cycle_stats(cycle_stats_df)`: Loads cycle_stats_df to `test_data_cycle_stats` table in the specified database.  

## Testing

To run the test suite, use the following commands:

- Run all tests: `pytest`
- Run Maccor tests: `pytest -m maccor`
- Run Arbin tests: `pytest -m arbin`
- Run database specific tests: `pytest -m database`
- Run tests other than Arbin: `pytest -m "not arbin"`
- Run Extractor tests: `pytest tests/test_extract_data.py`

To show logs while testing, add the `-o log_cli=true` flag.

## Troubleshooting

### pip install psycopg2 error

If there is an error `Error: pg_config executable not found.` Try installing `libpq-dev` before installing `psycopg2`

```sh
sudo apt install libpq-dev
```
