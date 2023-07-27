import os
import re
import sys
import json
import subprocess
import pandas as pd

from battetl import logger
from battetl.extract import Extractor
from battetl.transform import Transformer
from battetl.load import QuickLoader


def battetl_quick(file_path: str, file_meta = None) -> pd.DataFrame:
    '''
    The BattETL Quick Mode. Extracts test data or cycle stats from a file, transforms it,
    and loads it to the database.

    Parameters
    ----------
    file_path : str
        Name of the file to be processed.
    file_meta : dict
        A dictionary used to decode unstructured data.

    Returns
    -------
    pd.DataFrame
        The test data or cycle stats that was loaded to the database.
    '''

    extractor = Extractor()
    try:
        extractor.data_from_files([file_path], file_meta)
        raw_test_data = extractor.raw_test_data
        raw_cycle_stats = extractor.raw_cycle_stats
    except Exception as e:
        logger.error('Failed to extract test data', exc_info=True)

    transformer = Transformer()
    test_data = pd.DataFrame()
    cycle_stats = pd.DataFrame()
    if not raw_test_data.empty:
        try:
            transformer.transform_test_data(raw_test_data, file_meta)
            test_data = transformer.test_data
        except Exception as e:
            logger.error('Failed to transform test data', exc_info=True)
    elif not raw_cycle_stats.empty:
        try:
            transformer.transform_cycle_stats(raw_cycle_stats)
            cycle_stats = transformer.cycle_stats
        except Exception as e:
            logger.error('Failed to transform cycle stats', exc_info=True)

    loader = QuickLoader(file_path)
    if not test_data.empty:
        try:
            loader.load_test_data(test_data)
            logger.info(
                f'**** Data successfully loaded to test_data table under test_name {file_path} ****')
        except Exception as e:
            logger.error('Failed to load test data', exc_info=True)
    elif not cycle_stats.empty:
        try:
            loader.load_cycle_stats(cycle_stats)
            logger.info(
                f'**** Data successfully loaded to test_data_cycle_stats table under test_name {file_path} ****')
        except Exception as e:
            logger.error('Failed to load cycle stats', exc_info=True)

    if not test_data.empty:
        return test_data
    elif not cycle_stats.empty:
        return cycle_stats
    else:
        return pd.DataFrame()


def clone_battdb(battdb_folder: str = 'battdb'):
    '''
    Clones the BattDB repo to the specified folder.
    
    Parameters
    ----------
    battdb_folder : str, optional
        Name of the folder to clone battdb to. The default is 'battdb'.
    '''
    if not os.path.exists(battdb_folder):
        logger.info(f'Cloning battdb to {battdb_folder}...')
        subprocess.check_output(
            ['git', 'clone', '--depth', '1', 'https://github.com/BattGenie/battdb'])


def is_docker_installed() -> bool:
    '''
    Checks if Docker is installed.

    Returns
    -------
    bool
    '''
    try:
        logger.info('Checking if Docker is installed...')
        subprocess.check_output(['docker', '-v'])
        return True
    except:
        return False


def is_docker_compose_installed() -> bool:
    '''
    Checks if Docker Compose is installed.

    Returns
    -------
    bool
    '''
    try:
        subprocess.check_output(['docker-compose', '-v'])
        logger.info('Checking if Docker Compose is installed...')
        return True
    except:
        pass

    try:
        subprocess.check_output(['docker', 'compose', '-v'])
        return True
    except:
        pass

    return False


def run_docker_compose(battdb_folder: str = 'battdb') -> subprocess.CompletedProcess:
    '''
    Runs docker-compose up -d with the docker-compose.yml file in the BattDB repo.

    Parameters
    ----------
    battdb_folder : str, optional
        Name of the folder to clone BattDB to. The default is 'battdb'.

    Returns
    -------
    subprocess.CompletedProcess
    '''
    compose_file = os.path.join(
        os.getcwd(), battdb_folder, 'assets', 'battdb_docker', 'docker-compose.yml')
    logger.info('Bringing the database up and applying migrations.')
    logger.info(
        f'Running docker-compose up -d with compose file {compose_file}...')
    env = os.environ.copy()
    result = None
    try:
        result = subprocess.run([
            'docker-compose',
            '-f', compose_file,
            'up', '-d',
        ],
            env=env,
            capture_output=True,
            text=True,
        )
    except:
        result = subprocess.run([
            'docker', 'compose',
            '-f', compose_file,
            'up', '-d',
        ],
            env=env,
            capture_output=True,
            text=True,
        )

    return result


if __name__ == '__main__':
    n = len(sys.argv)
    print("num arguments" + str(n))
    if n < 3:
        print("Not enough arguments passed! Exiting!")
        sys.exit()
    elif n > 4:
        print("Too many arguments passed! Exiting!")
        sys.exit()
    else:
        for i in range(1, n):
            arg = str(sys.argv[i])
            if arg.startswith('file='):
                file_path = re.split('file=', arg)[-1]
            elif arg.startswith('db_url='):
                db_url = re.split('db_url=', arg)[-1]
            elif arg.startswith('file_meta='):
                file_meta_path = re.split('file_meta=', arg)[-1]
            else:
                print("Unknown argument " + arg + "! Exiting!")
                sys.exit()

    if not os.path.isfile(file_path):
        print("File path " + file_path + " does not exist! Exiting!")
        sys.exit()

    file_meta = None
    if os.path.isfile(file_meta_path):
        with open(file_meta_path) as file:
            file_meta = json.load(file)
    else:
        print("Invalid path for file_meta! Exiting!")
        sys.exit()

    # Assumes a db_url format of:
    # "postgres://YourUserName:YourPassword@YourHostname:Port/YourDatabaseName"
    split_list = re.split('//', db_url)[-1].split(':')
    username = split_list[0]
    password, hostname = split_list[1].split("@")
    port, dbname = split_list[2].split("/")
    battdb_folder = 'battdb'

    # Set the env variables based on the passed info.
    os.environ['DB_USERNAME'] = username
    os.environ['DB_PASSWORD'] = password
    os.environ['DB_HOSTNAME'] = hostname
    os.environ['DB_PORT'] = port
    os.environ['DB_TARGET'] = dbname
    # For docker-compose
    os.environ['POSTGRES_USER'] = username
    os.environ['POSTGRES_PASSWORD'] = password
    os.environ['POSTGRES_PORT'] = port
    os.environ['POSTGRES_DATABASE'] = dbname
    flyway_sql = os.path.join(
        os.getcwd(), battdb_folder, 'assets', 'migration_scripts_quick')
    os.environ['FLYWAY_SQL'] = flyway_sql

    if not is_docker_installed():
        print(f'Docker is not installed. Please go to https://www.docker.com/ to install Docker.')
        sys.exit()

    if not is_docker_compose_installed():
        print(f'Docker Compose is not installed. Please go to https://docs.docker.com/compose/ to install Docker Compose.')
        sys.exit()

    clone_battdb(battdb_folder)
    docker_result = run_docker_compose(battdb_folder)
    logger.info(docker_result.stdout)
    logger.info(docker_result.stderr)

    battetl_quick(file_path, file_meta)
