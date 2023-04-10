import os
import copy
import json
import psycopg2
import psycopg2.pool
import psycopg2.sql
import sqlalchemy
import sqlalchemy.orm
import numpy as np
import pandas as pd
from schema import Schema, Use, Optional, SchemaError

# progress bar
from tqdm import tqdm
# register the pandas extension
tqdm.pandas()

from battetl import logger, Constants, Utils


class Loader:
    def __init__(
            self,
            config: dict,
            env_path: str = os.path.join(os.getcwd(), '.env')):
        """
        An interface to load battery test data to a database.

        Parameters
        ----------
        config : dict
            A configuration file containing meta_data about the test data.
        env_path : str, optional
            Path to the environment file containing the database connection
            information. The default is '.env'. The environment file must
            contain the following fields:
                DB_TARGET - The target database to load data to.
                DB_USERNAME - The username to connect to the database.
                DB_PASSWORD - The password to connect to the database.
                DB_HOSTNAME - The hostname of the database.
                DB_PORT - The port of the database.
        """
        Utils.load_env(env_path)

        self.schema = Schema({
            'test_meta': {
                'test_name': str,
                'channel': Use(int),
                Optional('test_id'): Use(int),
                Optional('cell_id'): Use(int),
                Optional('start_date'): str,
                Optional('end_date'): str,
                Optional('ev_chamber'): Use(int),
                Optional('ev_chamber_slot'): Use(int),
                Optional('thermocouples'): Use(int),
                Optional('thermocouple_channels'): Use(int),
                Optional('comments'): str,
                Optional('project_id'): Use(int),
                Optional('test_capacity_mah'): Use(float),
                Optional('potentiostat_id'): Use(int),
                Optional('cycler_id'): Use(int),
            },
            'cycler': {
                'sn': str,
                Optional('cycler_id'): Use(int),
                Optional('cycler_type_id'): Use(int),
                Optional('calibration_date'): str,
                Optional('calibration_due_date'): str,
                Optional('location'): str,
                Optional('timezone_based'): str,
            },
            'cycler_meta': {
                'manufacturer': str,
                'model': str,
                Optional('cycler_type_id'): Use(int),
                Optional('manufacturer'): str,
                Optional('model'): str,
                Optional('datasheet'): str,
                Optional('num_channels'): Use(int),
                Optional('lower_current_limit_a'): Use(float),
                Optional('upper_current_limit_a'): Use(float),
                Optional('lower_voltage_limit_v'): Use(float),
                Optional('upper_voltage_limit_v'): Use(float),
            },
            'schedule_meta': {
                'schedule_name': str,
                'cycler_make': str,
                Optional('schedule_id'): Use(int),
                Optional('test_type'): str,
                Optional('cycler_make'): str,
                Optional('date_created'): str,
                Optional('created_by'): str,
                Optional('comments'): str,
                Optional('cv_voltage_threshold_mv'): Use(float),
                Optional('schedule_files'): str,
            },
            'cell': {
                'manufacturer_sn': str,
                Optional('cell_id'): Use(int),
                Optional('cell_type_id'): Use(int),
                Optional('batch_number'): str,
                Optional('label'): str,
                Optional('date_received'): str,
                Optional('comments'): str,
                Optional('date_manufactured'): str,
                Optional('manufacturer_sn'): str,
                Optional('dimensions'): str,
                Optional('weight_g'): Use(float),
                Optional('first_received_at_voltage_mv'): Use(float),
            },
            'cell_meta': {
                'manufacturer': str,
                'manufacturer_pn': str,
                Optional('cell_type_id'): Use(int),
                Optional('form_factor'): str,
                Optional('capacity_mah'): Use(float),
                Optional('chemistry'): str,
                Optional('dimensions'): str,
            },
        })

        assert (self.__validate_config(config))
        assert (self.__create_connection())

    def load_test_data(self, df: pd.DataFrame) -> int:
        """
        Loads test data to the target database. Only loads data past the
        latest `unixtime_s` field that already exists in the `test_data` table.

        Parameters
        ----------
        df : pd.DataFrame
            Data Frame containing test data to load to database.

        Returns
        -------
        num_rows_loaded : int
            The number of rows inserted into the `data` table.
        """

        df_copy = df.copy(deep=True)

        # Move fields to other_detail
        df_copy = self.__create_other_detail(
            df_copy, Constants.COLUMNS_TEST_DATA)

        for column in df_copy.columns:
            if column not in Constants.COLUMNS_TEST_DATA:
                logger.debug(f'Dropping column {column} from DataFrame')
                df_copy = df_copy.drop([column], axis=1)

        test_id = self.__lookup_test_id()
        if not test_id:
            test_id = self.__insert_test_meta()
        df_copy['test_id'] = np.ones(
            df_copy.shape[0], dtype=np.uint16) * test_id

        latest_unixtime_s = self.__lookup_latest_unixtime()
        if latest_unixtime_s:
            df_copy = df_copy[df_copy['unixtime_s'] > latest_unixtime_s]

        return self.__load_dataframe(df=df_copy, target_table='test_data')

    def load_cycle_stats(self, df: pd.DataFrame) -> int:
        """
        Loads cycle stats to the target database. Any cycles that already exist in the 
        database that overlap with the new cycle data will be overwritten. 

        Parameters
        ----------
        df : pd.DataFrame
            Data Frame containing test data to load to database.

        Returns
        -------
        num_rows_loaded : int
            The number of rows inserted into the `test_data` table.
        """

        df_copy = df.copy(deep=True)

        # Move fields to other_detail
        df_copy = self.__create_other_detail(
            df_copy, Constants.COLUMNS_CYCLE_STATS)

        for column in df_copy.columns:
            if column not in Constants.COLUMNS_CYCLE_STATS:
                logger.debug(f'Dropping column {column} from DataFrame')
                df_copy = df_copy.drop([column], axis=1)

        test_id = self.__lookup_test_id()
        if not test_id:
            test_id = self.__insert_test_meta()

        # Add the test_id as a column to the stats dataframe.
        df_copy['test_id'] = np.ones(
            df_copy.shape[0], dtype=np.uint16) * test_id

        # Get latest cycle_stats_id for the specific test_id
        with self.conn.cursor() as cursor:
            stmt = psycopg2.sql.SQL("""
                SELECT
                    cycle_stats_id, cycle
                FROM
                    test_data_cycle_stats
                WHERE
                    test_id = {test_id}
                AND 
                    cycle >= {newest_cycle}
                ORDER BY cycle_stats_id ASC
            """).format(
                test_id=psycopg2.sql.Literal(str(test_id)),
                newest_cycle=psycopg2.sql.Literal(str(int(df_copy.cycle.iloc[0])))
            )
            cursor.execute(stmt)
            result = cursor.fetchall()
        if result:
            duplicate_stats_id_bounds = [result[0][0], result[-1][0]]
            duplicate_cycle_range = [result[1][0], result[-1][0]]
            logger.info(
                f'Data through cycle {duplicate_cycle_range[-1]} already exists for test_id {test_id}')

        # Insert the new cycle stats data.
        num_rows_inserted = self.__load_dataframe(
            df=df_copy, target_table='test_data_cycle_stats')

        # If new data was inserted for cycles that previously existed then delete the old data.
        if num_rows_inserted and result:
            with self.conn.cursor() as cursor:
                stmt = psycopg2.sql.SQL("""
                    DELETE FROM 
                        test_data_cycle_stats
                    WHERE 
                        test_id = {test_id}
                    AND
                        cycle_stats_id 
                    BETWEEN
                        {duplicate_lower_bound}
                    AND 
                        {duplicate_upper_bound}
                """).format(
                    test_id=psycopg2.sql.Literal(str(test_id)),
                    duplicate_lower_bound=psycopg2.sql.Literal(
                        str(duplicate_stats_id_bounds[0])),
                    duplicate_upper_bound=psycopg2.sql.Literal(
                        str(duplicate_stats_id_bounds[1])),
                )
                cursor.execute(stmt)
            logger.info(
                f'Deleted old cycle_stats data for test_id {test_id} where cycle was greater than {df_copy.cycle.iloc[0]}')

        return num_rows_inserted

    def __validate_config(self, config: dict) -> bool:
        """
        Validates the passed configuration file for Loader class. 

        Parameters
        ----------
        config : dict
            Configuration for Loader class as defined in class description

        Returns
        -------
        valid : bool
            True if config is valid, False otherwise.
        """
        if not config:
            raise ValueError('Config cannot be empty.')
        if not 'meta_data' in config:
            raise ValueError('Config must contain meta_data.')

        try:
            self.schema.validate(config['meta_data'])
        except SchemaError as e:
            raise ValueError(e)

        self.config = config['meta_data']
        return True

    def __get_db_credentials(self):
        """
        Fetches database credentials from .env file. 

        Requires that the following environment variables are set:
            DB_TARGET,
            DB_USERNAME,
            DB_PASSWORD,
            DB_HOSTNAME,
            DB_PORT
        """
        # Validate that all required environment variables are set.
        if not os.getenv('DB_TARGET'):
            raise ValueError('DB_TARGET not set in environment variables.')
        if not os.getenv('DB_USERNAME'):
            raise ValueError('DB_USERNAME not set in environment variables.')
        if not os.getenv('DB_PASSWORD'):
            raise ValueError('DB_PASSWORD not set in environment variables.')
        if not os.getenv('DB_HOSTNAME'):
            raise ValueError('DB_HOSTNAME not set in environment variables.')
        if not os.getenv('DB_PORT'):
            raise ValueError('DB_PORT not set in environment variables.')

    def __create_connection(self) -> bool:
        """
        Creates connection to the target database specified in the config. 
        `self.conn` is the psycopg2 use for queries and limited meta insertion.
        `self.engine `is an sqlalchemy Engine instance used by Pandas for data
        frame insertion.

        Returns
        -------
        success : bool
            True or False based on whether the database connections were created.
        """
        self.__get_db_credentials()

        success = False
        try:
            postgreSQL_pool = psycopg2.pool.SimpleConnectionPool(
                minconn=1,
                maxconn=2,
                user=os.getenv('DB_USERNAME'),
                password=os.getenv('DB_PASSWORD'),
                host=os.getenv('DB_HOSTNAME'),
                port=os.getenv('DB_PORT'),
                database=os.getenv('DB_TARGET'),
            )
            self.conn = postgreSQL_pool.getconn()
            self.conn.autocommit = True
            self.engine = sqlalchemy.create_engine(
                'postgresql+psycopg2://', creator=postgreSQL_pool.getconn)
            success = True
            logger.info(
                f'Created connection to database {os.getenv("DB_TARGET")}')
        except psycopg2.OperationalError as e:
            logger.error(f'psycopg2 error creating connection to database {os.getenv("DB_TARGET")}!',
                         exc_info=True)
            logger.error(e)
        except sqlalchemy.exc.SQLAlchemyError as e:
            logger.error(f'sqlalchemy error creating connection to database {os.getenv("DB_TARGET")}!',
                         exc_info=True)
            logger.error(e)
        except Exception as e:
            logger.error(f'Unexpected error creating connection to database {os.getenv("DB_TARGET")}!',
                         exc_info=True)
            logger.error(e)

        return success

    def __create_other_detail(self, df: pd.DataFrame, table_columns: set) -> pd.DataFrame:
        """
        Creates the other_detail column in the DataFrame. This column contains
        all fields that are not in the target table.

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame to create other_detail column
        table_columns : set
            Set of columns in the target table

        Returns
        -------
        df : pd.DataFrame
            DataFrame with other_detail column
        """
        other_detail_columns = set(df.columns) - set(table_columns)

        if other_detail_columns:
            logger.info(
                f'Move fields to other_detail: {", ".join(other_detail_columns)}')
            df['other_detail'] = df.progress_apply(
                lambda row: json.dumps({
                    c: row[c] for c in other_detail_columns if not pd.isnull(row[c])
                }),
                axis=1,
            )

        return df

    # TODO: Modify query so that test_name is used in conjunction with start_date

    def __lookup_test_id(self) -> int:
        """
        Looks up the test_id in the target database based on the test_name defined
        within the config. If no entry exists then None is returned.

        Returns
        -------
        test_id : int
            The test_id from the target database. None if the test_id does not exist
        """
        with self.conn.cursor() as cursor:
            stmt = psycopg2.sql.SQL("""
                SELECT
                    test_id
                FROM
                    test_meta
                WHERE
                    test_name = {test_name}
            """).format(
                test_name=psycopg2.sql.Literal(
                    self.config['test_meta']['test_name'])
            )
            cursor.execute(stmt)
            test_id = cursor.fetchone()

        if test_id:
            test_id = test_id[0]

        logger.debug(f'Lookup test_id: {test_id}')
        return test_id

    def __lookup_cell_type_id(self) -> int:
        """
        Looks up the cell_id in the target database based on combination
        manufacturer and  manufacturer_sn as defined within the config. 
        If no entry exists then None is returned.

        Returns
        -------
        cell_id : int
            The cell_id from the target database. None if the test_id does not exist
        """

        with self.conn.cursor() as cursor:
            stmt = psycopg2.sql.SQL("""
                SELECT
                    cell_type_id
                FROM
                    cells_meta
                WHERE
                    manufacturer = {manufacturer}
                AND 
                    manufacturer_pn = {manufacturer_pn}
            """).format(
                manufacturer=psycopg2.sql.Literal(
                    self.config['cell_meta']['manufacturer']),
                manufacturer_pn=psycopg2.sql.Literal(
                    self.config['cell_meta']['manufacturer_pn'])
            )
            cursor.execute(stmt)
            cell_type_id = cursor.fetchone()

        # Pull value out of Tuple
        if cell_type_id:
            cell_type_id = cell_type_id[0]

        logger.debug(f'Lookup cell_type_id: {cell_type_id}')
        return cell_type_id

    def __lookup_cell_id(self) -> int:
        """
        Looks up the cell_id in the target database based on manufacturer,
        manufacturer_pn, and manufacturer_sn as defined within the config. 
        If no entry exists then None is returned.

        In future, consider making nested query rather than two queries to 
        speed up.

        Returns
        -------
        cell_id : int
            The cell_id from the target database. None if the test_id does not exist
        """

        cell_type_id = self.__lookup_cell_type_id()
        if not cell_type_id:
            return None

        with self.conn.cursor() as cursor:
            stmt = psycopg2.sql.SQL("""
                SELECT
                    cell_id
                FROM
                    cells
                WHERE
                    cell_type_id = {cell_type_id}
                AND 
                    manufacturer_sn = {manufacturer_sn}
            """).format(
                cell_type_id=psycopg2.sql.Literal(
                    cell_type_id),
                manufacturer_sn=psycopg2.sql.Literal(
                    self.config['cell']['manufacturer_sn'])
            )
            cursor.execute(stmt)
            cell_id = cursor.fetchone()

        if cell_id:
            cell_id = cell_id[0]

        logger.debug(f'Lookup cell_id: {cell_id}')
        return cell_id

    # TODO: Modify query so that date created is used in conjunction with schedule_name.
    def __lookup_schedule_id(self) -> int:
        """
        Looks up the schedule_id in the target database based on the 
        schedule schedule_name. Returns None if the schedule_id does not exist. 

        Returns
        -------
        schedule_id : int
            The schedule_id from the target database. None if the test_id does not exist
        """

        with self.conn.cursor() as cursor:
            stmt = psycopg2.sql.SQL("""
                SELECT
                    schedule_id
                FROM
                    schedule_meta
                WHERE
                    schedule_name = {schedule_name}
            """).format(
                schedule_name=psycopg2.sql.Literal(
                    self.config['schedule_meta']['schedule_name'])
            )
            cursor.execute(stmt)
            schedule_id = cursor.fetchone()

        # Pull value out of Tuple
        if schedule_id:
            schedule_id = schedule_id[0]

        logger.debug(f'Lookup schedule_id: {schedule_id}')
        return schedule_id

    def __lookup_cycler_type_id(self) -> int:
        """
        Looks up the cycler_type id in the target database based on the combination
        of manufacturer and model defined in the config.

        Returns
        -------
        cycler_type_id : int
            The cycler_type_id from the target database. None if the cycler_type_id does not exist.
        """

        with self.conn.cursor() as cursor:
            stmt = psycopg2.sql.SQL("""
                SELECT
                    cycler_type_id
                FROM
                    cyclers_meta
                WHERE
                    manufacturer = {manufacturer}
                AND 
                    model = {model}
            """).format(
                manufacturer=psycopg2.sql.Literal(
                    self.config['cycler_meta']['manufacturer']),
                model=psycopg2.sql.Literal(
                    self.config['cycler_meta']['model'])
            )
            cursor.execute(stmt)
            cycler_type_id = cursor.fetchone()

        # Pull value out of Tuple
        if cycler_type_id:
            cycler_type_id = cycler_type_id[0]

        logger.debug(f'Lookup cycler_type_id: {cycler_type_id}')
        return cycler_type_id

    def __lookup_cycler_id(self) -> int:
        """
        Looks up the cycler_id in the target database based on manufacturer,
        model, and cycler sn as defined within the config. 
        If no entry exists then None is returned.

        Returns
        -------
        cell_id : int
            The cell_id from the target database. None if the test_id does not exist
        """

        cycler_type_id = self.__lookup_cycler_type_id()
        if not cycler_type_id:
            return None

        with self.conn.cursor() as cursor:
            stmt = psycopg2.sql.SQL("""
                SELECT
                    cycler_id
                FROM
                    cyclers
                WHERE
                    cycler_type_id = {cycler_type_id}
                AND 
                    sn = {sn}
            """).format(
                cycler_type_id=psycopg2.sql.Literal(
                    cycler_type_id),
                sn=psycopg2.sql.Literal(
                    self.config['cycler']['sn'])
            )
            cursor.execute(stmt)
            cycler_id = cursor.fetchone()

        if cycler_id:
            cycler_id = cycler_id[0]

        logger.debug(f'Lookup cycler_id: {cycler_id}')
        return cycler_id

    def __insert_test_meta(self) -> int:
        """
        Inserts a new entry in test_meta table based on info in config.

        Returns
        -------
        test_id : int
            The test_id for the newly inserted test meta. None if insert failed.
        """

        cell_id = self.__lookup_cell_id()
        if not cell_id:
            logger.info(
                f'No cell_id exists for {json.dumps(self.config["cell"])}, creating new entry')
            cell_id = self.__insert_cell()

        schedule_id = self.__lookup_schedule_id()
        if not schedule_id:
            logger.info(
                f'No schedule_id exists for {json.dumps(self.config["schedule_meta"])}, creating new entry')
            schedule_id = self.__insert_schedule_meta()

        cycler_id = self.__lookup_cycler_id()
        if not cycler_id:
            logger.info(
                f'No cycler_id exists for {json.dumps(self.config["cycler"])}, creating new entry')
            cycler_id = self.__insert_cycler()

        upload_dict = copy.deepcopy(self.config['test_meta'])
        upload_dict['schedule_id'] = schedule_id
        upload_dict['cycler_id'] = cycler_id
        upload_dict['cell_id'] = cell_id

        logger.debug(f'Inserting test_meta: {json.dumps(upload_dict)}')
        return self.__perform_insert(target_table='test_meta', dict_to_load=upload_dict, pk_id_col='test_id')

    def __insert_cell(self) -> int:
        """
        Inserts a new entry in `cells` table based on info in config.

        Returns
        -------
        cell_id : int
            The cell_id for the newly inserted cell. None if the insert failed.
        """
        cell_type_id = self.__lookup_cell_type_id()
        if not cell_type_id:
            logger.info(
                f'No cell_type_id exists for {json.dumps(self.config["cell_meta"])}, creating new entry')
            cell_type_id = self.__insert_cell_meta()

        upload_dict = copy.deepcopy(self.config['cell'])
        upload_dict['cell_type_id'] = cell_type_id

        logger.debug(f'Inserting cell: {json.dumps(upload_dict)}')
        return self.__perform_insert(target_table='cells', dict_to_load=upload_dict, pk_id_col='cell_id')

    def __insert_cell_meta(self) -> int:
        """
        Inserts a new entry in `cell_meta` table based on info in config.

        Returns
        -------
        cell_type_id : int
            The cell_type_id for the newly inserted cell_meta. None if the insert failed.
        """
        upload_dict = copy.deepcopy(self.config['cell_meta'])

        logger.debug(f'Inserting cell_meta: {json.dumps(upload_dict)}')
        return self.__perform_insert(target_table='cells_meta', dict_to_load=upload_dict, pk_id_col='cell_type_id')

    def __insert_schedule_meta(self) -> int:
        """
        Inserts a new entry in `schedule_meta` table based on info in the config.

        Returns
        -------
        schedule_id : int
            The schedule_id for the newly inserted schedule meta. None if the insert failed.
        """
        upload_dict = copy.deepcopy(self.config['schedule_meta'])

        logger.debug(f'Inserting schedule_meta: {json.dumps(upload_dict)}')
        return self.__perform_insert(target_table='schedule_meta', dict_to_load=upload_dict, pk_id_col='schedule_id')

    def __insert_cycler(self) -> int:
        """
        Inserts a new entry in `cycles` table based on info in config.

        Returns
        -------
        cycler_id : int
            The cycler_id for the newly inserted cycler. None if the insert failed.
        """
        cycler_type_id = self.__lookup_cycler_type_id()
        if not cycler_type_id:
            logger.info(
                f'No cycler_type_id exists for {json.dumps(self.config["cycler_meta"])}, creating new entry')
            cycler_type_id = self.__insert_cycler_meta()

        upload_dict = copy.deepcopy(self.config['cycler'])
        upload_dict['cycler_type_id'] = cycler_type_id

        logger.debug(f'Inserting cycler: {json.dumps(upload_dict)}')
        return self.__perform_insert(target_table='cyclers', dict_to_load=upload_dict, pk_id_col='cycler_id')

    def __insert_cycler_meta(self) -> int:
        """
        Inserts a new entry in `cyclers_meta` table based on info in config.

        Returns
        -------
        cycler_type_id : int
            The cycler_type_id for the new inserted row.
        """
        upload_dict = copy.deepcopy(self.config['cycler_meta'])

        logger.debug(f'Inserting cycler_meta: {json.dumps(upload_dict)}')
        return self.__perform_insert(target_table='cyclers_meta', dict_to_load=upload_dict, pk_id_col='cycler_type_id')

    def __perform_insert(self, target_table: str, dict_to_load: dict, pk_id_col: str) -> int:
        """
        Inserts the values from the dict_to_load into the `target_table`. Assumes
        columns are keys. Drops any empty keys.

        Returns
        -------
        target_table : str
            The table to perform the insert into.
        dict_to_load : dict
            The dictionary containing the key value pairs to load into the target table.
        pk_id_col : str
            The name of the column for the primary key id.
        Returns
        -------
        pk_id : int
            The primary key id for the newly inserted row. Returns None if issue with inserting rows.
        """

        # Remove any empty entries from upload dict
        dict_to_load = {k: v for k, v in dict_to_load.items() if v}
        logger.debug(f'Inserting into {target_table} with {json.dumps(dict_to_load)}')
        pk_id = None

        try:
            insert_statement = psycopg2.sql.SQL(
                "INSERT INTO {table} ({cols}) VALUES ({vals}) RETURNING {pk_id}").format(
                table=psycopg2.sql.Identifier(target_table),
                cols=psycopg2.sql.SQL(', ').join(
                    map(psycopg2.sql.Identifier, list(dict_to_load.keys()))),
                vals=psycopg2.sql.SQL(', ').join(
                    psycopg2.sql.Placeholder() * len(dict_to_load)),
                pk_id=psycopg2.sql.Identifier(pk_id_col)
            )
            with self.conn.cursor() as cursor:
                cursor.execute(insert_statement, list(dict_to_load.values()))
                result = cursor.fetchone()
            if result:
                pk_id = result[0]
                logger.info(
                    f'Inserted new data into {target_table} with pk_id_col={pk_id_col} and pk_id={pk_id}')
            else:
                logger.error(
                    f'No result returned when inserting into {target_table} with pk_id_col={pk_id_col}')
        except psycopg2.Error as e:
            logger.error(
                f'Error inserting into {target_table} with pk_id_col={pk_id_col}')
            logger.error(e)
        except Exception as e:
            logger.error(f'Unexpected error inserting into {target_table} with pk_id_col={pk_id_col}')
            logger.error(e)

        return pk_id

    def __lookup_latest_unixtime(self) -> int:
        """
        Looks up the latest `unixtime_s` loaded into the `test_data` table for the
        test specified in the config. If there is no entry `unixtime_s` in the 
        data table then None is returned. 

        Returns
        -------
        latest_unixtime_s : float
            The latest unixtime_s in the `test_data` table for the test.
        """

        test_id = self.__lookup_test_id()
        if not test_id:
            logger.info(
                f'No test data exists for {test_id}, OK to upload all data.')
            return None

        with self.conn.cursor() as cursor:
            cursor.execute("""
                SELECT
                    MAX(unixtime_s)
                FROM
                    test_data
                WHERE
                    test_id = %(test_id)s
            """, {
                'test_id': str(test_id)
            })
            latest_unixtime_s = cursor.fetchone()

        if latest_unixtime_s:
            latest_unixtime_s = latest_unixtime_s[0]

        logger.info(
            f'Latest unixtime_s for test_id={test_id} is {latest_unixtime_s}')

        return latest_unixtime_s

    def __lookup_latest_cycle(self) -> int:
        """
        Looks up the latest `cycle` loaded into the `test_data_cycle_stats` table
        for the test specified in the config. If there is no entry for `cycle` in the 
        table then None is returned. 

        Returns
        -------
        latest_cycle : int
            The latest cycle uploaded in the database for the test.
        """

        test_id = self.__lookup_test_id()
        if not test_id:
            logger.info(
                f'No test data exists for {test_id}, OK to upload all data.')
            return None

        with self.conn.cursor() as cursor:
            cursor.execute("""
                SELECT
                    MAX(cycle)
                FROM
                    test_data_cycle_stats
                WHERE
                    test_id = %(test_id)s
            """, {
                'test_id': str(test_id)
            })
            latest_cycle = cursor.fetchone()

        if latest_cycle:
            latest_cycle = latest_cycle[0]

        logger.info(f'Latest cycle for test_id={test_id} is {latest_cycle}')

        return latest_cycle

    def __load_dataframe(self, df: pd.DataFrame, target_table: str) -> int:
        """
        Loads the passed data frame to the passed target_table in the database
        specified in the config.

        Parameters
        ----------
        df : pd.DataFrame
            Data frame to load to database.
        target_table : str
            Table to load the data frame to.

        Returns
        -------
        num_rows_inserted : int
            The number of rows inserted into the target_table.
        """
        __upload_chunk_size = 10000
        num_rows_inserted = 0

        def chunker(seq, size):
            # from http://stackoverflow.com/a/434328
            return (seq[pos:pos + size] for pos in range(0, len(seq), size))

        try:
            logger.info(f'Inserting {len(df)} rows into {target_table} table.')
            with tqdm(total=len(df)) as pbar:
                for chunk in chunker(df, __upload_chunk_size):
                    num_rows_inserted += chunk.to_sql(
                        name=target_table,
                        con=self.engine,
                        schema='public',
                        if_exists='append',
                        index=False,  # Do not include the pd table index as a column
                        # Pass multiple values in a single INSERT clause.
                        method='multi'
                    )
                    pbar.update(len(chunk))

            logger.info(
                f'Inserted {num_rows_inserted} rows into {target_table} table.')
        except psycopg2.OperationalError as e:
            logger.error(
                f'Error inserting into {target_table} table')
            logger.error(e)
        except Exception as e:
            logger.error(
                f'Unexpected error inserting into {target_table} table.')
            logger.error(e)

        return num_rows_inserted
