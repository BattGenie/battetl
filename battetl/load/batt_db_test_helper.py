import pandas as pd
import random
import string
from psycopg2 import sql
from copy import deepcopy

from .Loader import Loader


class BattDbTestHelper(Loader):

    cell_type_id = None
    cell_id = None
    cycler_type_id = None
    cycle_id = None
    schedule_id = None
    test_id = None
    profile_id = None
    customer_id = None

    def __init__(self, config: dict):
        '''
        Various methods to help with tests involving BattDB
        '''
        super().__init__(config)
        self.config = config['meta_data']
        assert (self._Loader__create_connection())

    def create_test_db_entries(self):
        '''
        Creates various entries into the database that the tests assume exist.
        Assert to make sure the inserts actually happened. If they fail, then
        likely there is data in the database that needs to be cleared. Easiest course
        of action in that case is rebuilding the database from scratch.
        '''
        self.cell_type_id = self._Loader__insert_cell_meta()
        assert (self.cell_type_id)

        upload_dict = {**self.config['cell'],
                       **{'cell_type_id': self.cell_type_id}}
        self.cell_id = self._perform_insert(
            target_table='cells', dict_to_load=upload_dict, pk_id_col='cell_id')
        assert (self.cell_id)

        self.cycler_type_id = self._perform_insert(
            target_table='cyclers_meta', dict_to_load=self.config['cycler_meta'], pk_id_col='cycler_type_id')
        assert (self.cycler_type_id)

        upload_dict = {**self.config['cycler'],
                       **{'cycler_type_id': self.cycler_type_id}}
        self.cycler_id = self._perform_insert(
            target_table='cyclers', dict_to_load=upload_dict, pk_id_col='cycler_id')
        assert (self.cycler_id)

        self.schedule_id = self._perform_insert(
            target_table='schedule_meta', dict_to_load=self.config['schedule_meta'], pk_id_col='schedule_id')
        assert (self.schedule_id)

        upload_dict = {**self.config['customers']}
        self.customer_id = self._perform_insert(
            target_table='customers', dict_to_load=upload_dict, pk_id_col='customer_id')
        assert (self.customer_id)

        upload_dict = {**self.config['projects'],
                       'customer_id': self.customer_id}
        self.project_id = self._perform_insert(
            target_table='projects', dict_to_load=upload_dict, pk_id_col='project_id')
        assert (self.project_id)

        upload_dict = {**self.config['test_meta'],
                       **{'cycler_id': self.cycler_id,
                          'schedule_id': self.schedule_id,
                          'cell_id': self.cell_id}}
        self.test_id = self._perform_insert(
            target_table='test_meta', dict_to_load=upload_dict, pk_id_col='test_id')
        assert (self.test_id)

        upload_dict = {'test_id': self.test_id,
                       'user_id': 'test_helper',
                       'cell_type_id': self.cell_type_id}
        self.sil_id = self._perform_insert(
            target_table='sil_meta', dict_to_load=upload_dict, pk_id_col='sil_id')

        upload_dict = {'test_id': self.test_id,
                       'user_id': 'test_helper',
                       'cell_type_id': self.cell_type_id}
        self.sim_id = self._perform_insert(
            target_table='sim_meta', dict_to_load=upload_dict, pk_id_col='sim_id')
        assert (self.test_id)

    def read_first_row(self, target_table: str, pk_col_name: str) -> tuple:
        """
        Returns the first row in the target_table, sorting off of pk_col_name.

        Parameters
        ----------
        target_table : str
            The table pull the first row from
        pk_col_name : str
            The name of the column for the primary key id.

        Returns
        -------
        pk_id : int
            The primary key id for the newly inserted row. Returns None if issue with inserting rows.
        """
        with self._conn.cursor() as cursor:
            stmt = sql.SQL("""
            SELECT
                *
            FROM
                {table}
            ORDER BY
                {pk_col_name}
            ASC LIMIT 1;""").format(
                table=sql.Identifier(target_table),
                pk_col_name=sql.Identifier(pk_col_name),
            )
            cursor.execute(stmt)
            first_row = cursor.fetchone()

        return first_row

    def read_last_row(self, target_table: str, pk_col_name: str) -> tuple:
        """
        Returns the last row in the target_table, sorting off of pk_col_name.

        Parameters
        ----------
        target_table : str
            The table pull the last row from
        pk_col_name : str
            The name of the column for the primary key id.

        Returns
        -------
        pk_id : int
            The primary key id for the newly inserted row. Returns None if issue with inserting rows.
        """
        with self._conn.cursor() as cursor:
            stmt = sql.SQL("""
            SELECT
                *
            FROM
                {table}
            ORDER BY
                {pk_col_name}
            DESC LIMIT 1;""").format(
                table=sql.Identifier(target_table),
                pk_col_name=sql.Identifier(pk_col_name),
            )
            cursor.execute(stmt)
            last_row = cursor.fetchone()

        return last_row

    def load_sil_data(self, df: pd.DataFrame) -> int:
        """
        Loads the passed sil_data to the database.

        Parameters
        ----------
        df : pd.DataFrame
            Dataframe with data to load to `sil_data` table

        Returns
        -------
        num_rows_loaded : int
            The number of rows loaded to the `sil_data` table
        """
        df['sil_id'] = self.sil_id
        return self._load_dataframe(df, 'sil_data')

    def load_sim_data(self, df: pd.DataFrame) -> int:
        """
        Loads the passed sim_data to the database.

        Parameters
        ----------
        df : pd.DataFrame
            Dataframe with data to load to `sim_data` table

        Returns
        -------
        num_rows_loaded : int
            The number of rows loaded to the `sim_data` table
        """
        df['sim_id'] = self.sim_id
        return self._load_dataframe(df, 'sim_data')

    def load_df_to_db(self, df: pd.DataFrame, target_table: str) -> int:
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
        return self._load_dataframe(df, target_table)

    def delete_test_db_entries(self):
        '''
        Deletes the test_db entries created in `self.create_test_db_entries`
        '''
        self.delete_entry(
            'sil_meta', 'sil_id', self.sil_id)
        self.delete_entry(
            'sim_meta', 'sim_id', self.sim_id)
        self.delete_entry(
            'test_meta', 'test_id', self.test_id)
        self.delete_entry(
            'cells', 'cell_id', self.cell_id)
        self.delete_entry(
            'cells_meta', 'cell_type_id', self.cell_type_id)
        self.delete_entry(
            'cyclers', 'cycler_id', self.cycler_id)
        self.delete_entry(
            'cyclers_meta', 'cycler_type_id', self.cycler_type_id)
        self.delete_entry(
            'schedule_meta', 'schedule_id', self.schedule_id)
        self.delete_entry(
            'projects', 'project_id', self.project_id)
        self.delete_entry(
            'customers', 'customer_id', self.customer_id)

    def delete_entry(self, target_table, pk_col_name, pk_id):
        '''
        Drops an entry from a meta table.

        Parameters
        ----------
        target_table : str
            The name of the meta table. E.g. "cells_meta"
        pk_col_name : str
            The name of the primary key column. E.g. "cell_type_id"
        pk : int
            The primary key id number. E.g 1
        '''
        with self._conn.cursor() as cursor:
            stmt = sql.SQL("""
                DELETE FROM
                    {table_name}
                WHERE
                    {pk_col_name} = {pk_id}
            """).format(
                table_name=sql.Identifier(target_table),
                pk_col_name=sql.Identifier(pk_col_name),
                pk_id=sql.Literal(pk_id)
            )
            cursor.execute(stmt)

    def delete_test_data(self):
        '''
        Deletes all data from from data tables
        '''
        data_to_delete_info = [
            ('test_data', 'test_id', self.test_id),
            ('test_data_cycle_stats', 'test_id', self.test_id),
            ('sil_data', 'sil_id', self.sil_id),
            ('sim_data', 'sim_id', self.sim_id),
        ]
        for data_info in data_to_delete_info:
            self.delete_entry(data_info[0], data_info[1], data_info[2])

    def generate_random_string(self, len=20) -> str:
        '''
        Generates a random string of length `len`.

        Parameters
        ----------
        len : int
            The desired length of the random string

        Returns
        -------
        random_string : str
            A random string of length `len`.
        '''
        return (''.join(random.choice(string.ascii_letters) for i in range(len)))
