import psycopg2.sql
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
        likely there is data in the database thats needs to be cleared. Easiest course 
        of action in that case is rebuilding the database from scratch. 
        '''
        self.cell_type_id = self.perform_insert(
            target_table='cells_meta', dict_to_load=self.config['cell_meta'], pk_id_col='cell_type_id')
        assert (self.cell_type_id)

        upload_dict = {**self.config['cell'],
                       **{'cell_type_id': self.cell_type_id}}
        self.cell_id = self.perform_insert(
            target_table='cells', dict_to_load=upload_dict, pk_id_col='cell_id')
        assert (self.cell_id)

        self.cycler_type_id = self.perform_insert(
            target_table='cyclers_meta', dict_to_load=self.config['cycler_meta'], pk_id_col='cycler_type_id')
        assert (self.cycler_type_id)

        upload_dict = {**self.config['cycler'],
                       **{'cycler_type_id': self.cycler_type_id}}
        self.cycler_id = self.perform_insert(
            target_table='cyclers', dict_to_load=upload_dict, pk_id_col='cycler_id')
        assert (self.cycler_id)

        self.schedule_id = self.perform_insert(
            target_table='schedule_meta', dict_to_load=self.config['schedule_meta'], pk_id_col='schedule_id')
        assert (self.schedule_id)

        upload_dict = {**self.config['test_meta'],
                       **{'cycler_id': self.cycler_id,
                          'schedule_id': self.schedule_id,
                          'cell_id': self.cell_id}}
        self.test_id = self.perform_insert(
            target_table='test_meta', dict_to_load=upload_dict, pk_id_col='test_id')
        assert (self.test_id)

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
        with self.conn.cursor() as cursor:
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

    def perform_insert(self, target_table: str, dict_to_load: dict, pk_id_col: str) -> int:
        """
        Inserts the values from the dict_to_load into the `target_table`. Assumes
        columns are keys. Drops any empty keys.

        Parameters
        ----------
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
        {k: v for k, v in dict_to_load.items() if v}

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

        return pk_id

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
        __upload_chunk_size = 10000

        num_rows_inserted = df.to_sql(
            name=target_table,
            con=self.engine,
            schema='public',
            if_exists='append',
            index=False,
            chunksize=__upload_chunk_size,
            method='multi')

        return num_rows_inserted

    def delete_test_db_entries(self):
        '''
        Deletes the test_db entries created in `self.create_test_db_entries`
        '''
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
        with self.conn.cursor() as cursor:
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
        Deletes all data from the test_data and test_data_cycle_stats tables for test specified in config. 
        '''
        with self.conn.cursor() as cursor:
            stmt = sql.SQL("""
                DELETE FROM 
                    test_data
                WHERE 
                    test_id = {test_id}
            """).format(
                test_id=sql.Literal(self.test_id)
            )
            cursor.execute(stmt)

        with self.conn.cursor() as cursor:
            stmt = sql.SQL("""
                DELETE FROM 
                    test_data_cycle_stats
                WHERE 
                    test_id = {test_id}
            """).format(
                test_id=sql.Literal(self.test_id)
            )
            cursor.execute(stmt)

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
