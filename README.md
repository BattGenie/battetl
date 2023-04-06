# BattETL

BattETL is a well-tested and an enterprise-ready python module for **E**xtracting, **T**ransforming, and **L**oading battery cycler data to a [database](https://github.com/BattGenie/battdb). BattETL can also be used just for data extraction and transformation if a database is not desired. Currently data from Maccor and Arbin cyclers are supported.

<img src="./diagrams/BattETL_Simple_System.svg">

### Why another battery cycler data ingesting tool?

There are published [open](https://github.com/TRI-AMDD/beep) [source](https://github.com/Battery-Intelligence-Lab/galvanalyser) solutions for battery cycler data ingestion. BattETL hopes to build on these tools and adds a few conveniences to support robust data management and repeatable data analytics:

Some features of BattETL:

- **A relational data destination.**

BattETL package is tightly coupled to the destination database it sends the data to. BattETL requires that for the battery data that is loaded to the database - the detailed metadata such as battery make and model, cycler make and model, and schedule/procedure information be also included. While these constraints (somewhat) slowdown the data ingestion process, it ensures verifiable, high-quality data persistance.

- **A database that can hold *all* relevant battery test information.**

Not only does the BattETL extract and store all the battery test data, it also captures the cycler schedule/procedure file. In addition to the schedule/procedure file, BattETL captures and stores all files associated with the schedule/procedure. This includes any current profiles (Maccor FastWave or Arbin current simulation files), EIS specifications, or even entire system specifications (Arbin batch files.)

- **Analytics built into the transformation step.**

As part of the transformation step BattETL calculates various cycle statistics (capacity, coulombic efficiency, CC/CV charge times, etc.) providing independent and supplemental cycle statistics to compliment those generated by the cycler software. These can particularly handy when easy-to-miss settings were overlooked in writing the schedule (e.g. forgetting to reset the capacity when incriminating the cycle.) Do you have some sort of hyper-boutique transformation or statistic calculation needed for a specific test? No problem. BattETL can apply user defined transformation functions too; all before the data is loaded to the database.

### System diagram of BattETL and BattDB

<img src="./diagrams/BattETL_SystemDiagram.svg">

## Requirements

BattETL requires Python 3.9 or 3.10.

## Installation

```sh
pip install -r requirements.txt
pip install .
```

## Data export requirements

### Maccor

From the "Maccor Export" tool data should be exported as "Text Output" in a "Tab Delimited" format. The output sheets should include "Data Records" and "Cycle Statistics". The box for "Discharge Current Exported Negative" should be checked.

The following columns are the minimum required for export:

For Data Records:

- Cycle : Header Label of Cyc#
- Step : Header Label of Step
- Test Time : Header Label of TestTime(s), Units of seconds
- Step Time : Header Label of StepTime(s), Units of seconds
- Capacity : Header Label of Capacity(Ah), Units of Amp-Hour
- Energy : Header Label of Watt-hr, Units of Whatt-hour
- Current : Header Label of Current(A), Units of Amps
- Voltage : Header Label of Voltage(V), Units of Volts
- DPT Time : Header label of DPt Time

For Cycle Statistics:

Cycle : Header Label of Cycle
Capacity Chg : Header label AH-IN, Units of Ahr
Capacity Dis : Header label AH-Out, Units of Ahr
DPT Time : Header Label of DPt Time

### Arbin

Within the Arbin Export tool the "File Type" should be set to "To CSV". Within "Data Filter" the box "statistics by Cycle" should be checked along with any other data. For range filter "All" should be selected.

## Usage

### BattETL

Using BattETL as an all-in-one ETL function is as easy as:

```python
from battetl import BattETL

cell = BattETL(config_path).extract().transform().load()
```

Where `config_path` is the absolute or relative path to BattETL config file (detailed below.)

When loading data to a database it is also necessary to have an .env file within the working directory that contains the associated database credentials. An example .env file ( `.env.example`) is given in the root directory of this repository.

#### Config File

To use BattETL it is necessary to provide a path to JSON configuration file. This config file contains paths to the relevant test data files and test metadata used for analysis and establishing database relations. An example configuration file is shown below:
  
***Config file example***

```json
{
    "timezone": "America/Los_Angeles",
    "data_file_path": [
        "data/test_data.txt"
    ],
    "stats_file_path": [
        "data/test_stats.txt"
    ],
    "schedule_file_path": [
        "data/fake_schedule.001"
    ],
    "target_databases": [
        "test_db"
    ],
    "meta_data": {
        "test_meta": {
            "test_name": "TEST_Cell1_Take1",
            "channel": 1
        },
        "cell": {
            "manufacturer_sn": "123456"
        },
        "cell_meta": {
            "manufacturer": "FakeMN",
            "manufacturer_pn": "1234"
        },
        "schedule_meta": {
            "schedule_name": "fake_schedule.001",
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
```

#### BattETL Class Instance Objects

After running BattETL on a specific configuration file the following objects will be available as class variables from the instance:

`raw_test_data: pandas.DataFrame`: The raw test data exactly as it is in the generated cycler data file(s).
`raw_cycle_stats: pandas.DataFrame`: The raw cycle stats data exactly as it is in the generated cycler statistics file.
`test_data: pandas.DataFrame`: The transformed test data after normalizing units and datatype and adding a unixtime column.
`cycle_stats: pandas.DataFrame`: The transformed cycle stats after normalizing units, datatypes, and calculating supplemental cycle statistics.
`schedule: dict`: The procedure/schedule file and all associated files in a nested Dictionary.

***Example Notebook***

For a practical implementation of `BattETL`, please refer to the example notebook `load_data.ipynb` located in the `tests/data` directory. This notebook demonstrates how to implement `BattETL` and can serve as a useful reference for your own project.

### Extractor

The Extractor is an interface that allows you to extract battery test data from raw data files. You can extract test data and cycle stats data from multiple files using `data_from_files`, and extract Arbin schedules or Maccor procedures and associated files using `schedule_from_files`.

```python
from battetl.extract import Extractor

extractor = Extractor()

# Extract test data and cycle stats
paths = [
    'foo/bar_1.txt',
    'foo/bar_2.txt',
]

extractor.data_from_files(paths)

raw_test_data = extractor.raw_test_data
raw_cycle_stats = extractor.raw_cycle_stats

# Extract schedule
paths = [
    'foo/bar_1.sdx',
]

extractor.schedule_from_files(paths)

maccor_procedure = self.self.maccor_procedure
arbin_schedule = self.arbin_schedule
```

#### Functions

`data_from_files(paths: list[str])`: Extracts multiple test data files into a single pandas DataFrame.  
`schedule_from_files(paths: list[str])`: Extracts Arbin schedules or Maccor procedures and associated files and stores them in a dictionary.  
`from_pickle(path: str)`: Reads data from the passed file path and returns it as a pandas DataFrame.  

#### Variables

`cycler_make: str`: Cycler make  
`raw_test_data_meta_data: list[dict]`: Test data meta data  
`raw_cycle_stats_meta_data: list[dict]`: Cycler stats meta data  
`raw_test_data: pandas.DataFrame`: Test data  
`raw_cycle_stats: pandas.DataFrame`: Cycler stats  
`maccor_procedure: dict`: Maccor procedure  
`arbin_schedule: dict`: Arbin schedule  

### Transformer

The Transformer is an interface that allows you to transform battery test data to the BattETL schema. By default, the time zone is set to 'America/Los_Angeles', but you can modify it to your preferred time zone using the names found in the [IANA Time Zone Database](https://www.iana.org/time-zones).

```python
from battetl.transform import Transformer

transformer = Transformer(timezone='America/Los_Angeles')
transformer.transform_test_data(extractor.raw_test_data)
transformer.transform_cycle_stats(extractor.raw_cycle_stats)
```

In addition to the default functions provided by the Transformer, you can add your own custom functions to perform specific transformations on your data. For example, you may want to convert seconds to minutes. Here's an example of how you can add a custom function to do just that:

```python
import pandas as pd
from battetl.transform import Transformer

def test_time_to_min(df: pd.DataFrame) -> pd.DataFrame:
    df['test_time_min'] = df['test_time_s'] / 60
    return df

transformer = Transformer(timezone='America/Los_Angeles', user_transform_cycle_stats=test_time_to_min)
transformer.transform_cycle_stats(extractor.raw_cycle_stats)
```

By adding the custom function `test_time_to_min` to the `Transformer` object, you can now call it when you transform your cycle stats data. This is just one example of how you can extend the functionality of the Transformer to suit your needs. With this interface, you can easily transform your battery test data to the BattETL schema and perform any additional transformations you need to prepare your data for analysis.

If you want to chain multiple custom transforms, you can create a pipeline of functions using the `compose` function from the `toolz` library:

```python
import pandas as pd
from toolz import compose
from battetl.transform import Transformer

def test_time_to_min(df: pd.DataFrame) -> pd.DataFrame:
    df['test_time_min'] = df['test_time_s'] / 60
    return df

def test_time_to_hour(df: pd.DataFrame) -> pd.DataFrame:
    df['test_time_hour'] = df['test_time_s'] / 3600
    return df

# create pipeline of functions
pipeline = compose(test_time_to_min, test_time_to_hour)

# pass pipeline to Transformer constructor
transformer = Transformer(timezone='America/Los_Angeles', user_transform_cycle_stats=pipeline)
transformer.transform_cycle_stats(extractor.raw_cycle_stats)
```

#### Functions

`transform_test_data(self, data: pd.DataFrame)`: Transforms test data to conform to BattETL naming and data conventions  
`transform_cycle_stats`: Transforms cycle stats to conform to BattETL naming and data conventions  

#### Variables

`timezone: str`: Time zone strings in the IANA Time Zone Database. Used to convert to unix timestamp in seconds. Default 'America/Los_Angeles'.  
`user_transform_test_data`: A user defined function to transform test data. The function should take a pandas.DataFrame as input and return a pandas.DataFrame as output.  
`user_transform_cycle_stats`: A user defined function to transform cycle stats. The function should take a pandas.DataFrame as input and return a pandas.DataFrame as output.  
`test_data: pandas.DataFrame`: Transformed test data  
`cycle_stats: pandas.DataFrame`: Transformed cycle stats  

### Load

The `load` module provides an interface to load the extracted data into a database.

Before running the `load` module, copy `.env.example` to `.env` and modify the parameters accordingly.

```python
from battetl.extract import Loader

# Database to load data to.
target_database = "test_database"

# Config describing test parameters
config = {
    "test_meta":
        {
            "test_name": "Fake_Test",
        },
    "cell":
        {
            "manufacturer_sn": "0001"
        },
    "cell_meta":
        {
            "manufacturer": "FakeMN",
            "manufacturer_pn": "1234"
        },
    "schedule_meta":
        {
            "file_name": "fake_schedule.000"
        },
    "cycler":
        {
            "sn": "0001"
        },
    "cycler_meta":
        {
            "manufacturer": "BattGenie",
            "model": "Cycler9000"
        }
}

loader = Loader(target_database, config)

# Where `test_data_df` is a DataFrame that conforms to `test_data` columns and data types
loader.load_test_data(test_data_df)

# Where `cycle_stats_df` is a DataFrame that conforms to `test_data_cycle_stats` columns and data types
loader.load_cycle_stats(cycle_stats_df)
```

#### Functions

`load_test_data(test_data_df)`: Loads test_data_df to `test_data` table in the specified database.  
`load_cycle_stats(cycle_stats_df)`: Loads cycle_stats_df to `test_data_cycle_stats` table in the specified database.  

## Test

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
