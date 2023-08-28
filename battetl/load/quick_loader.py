import pandas as pd
import json
import os
import copy
from .Loader import Loader
from battetl import logger, Constants


class QuickLoader(Loader):

    def __init__(self, file_path):

        # The config needed by the BattETL parent class
        self._file_name = os.path.basename(file_path)
        config = {
            "meta_data": {
                "test_meta": {
                    "test_name": copy.deepcopy(self._file_name),
                    "channel": "0"
                },
                "cell": {
                    "manufacturer_sn": "NA"
                },
                "cell_meta": {
                    "manufacturer": "NA",
                    "manufacturer_pn": "NA",
                },
                "schedule_meta": {
                    "schedule_name": "NA",
                    "cycler_make": "NA"
                },
                "cycler": {
                    "sn": "NA",
                },
                "cycler_meta": {
                    "manufacturer": "NA",
                    "model": "NA",
                },
                "customers": {
                    "customer_name": "NA",
                },
                "projects": {
                    "project_name": "NA",
                }
            }
        }
        super().__init__(config=config, battdb_version=Constants.BATTDB_QUICK_SCHEMA_VERSION)

    def load_test_data(self, df: pd.DataFrame, retry_cnt: int = 0) -> int:
        """
        Loads test data to the target database. Only loads data past the
        latest `unixtime_s` field that already exists in the `test_data` table.

        Parameters
        ----------
        df : pd.DataFrame
            Data Frame containing test data to load to database.
        retry_cnt : int, optional
            The number of times to retry loading data to the database.

        Returns
        -------
        num_rows_loaded : int
            The number of rows inserted into the `data` table.
        """
        # If a test_meta entry does not already exist, then create it.
        if not super()._lookup_test_id():
            assert (self._insert_quick_test_meta(file_name=self._file_name))
        return super().load_test_data(df, retry_cnt)

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
        if not super()._lookup_test_id():
            assert (self._insert_quick_test_meta(file_name=self._file_name))
        return super().load_cycle_stats(df)

    def _insert_quick_test_meta(self, file_name):
        '''
        Creates a test_meta entry for the passed file name. The only entry populated is the test_name column, which is set to the file name.
        '''
        upload_dict = {'test_name': file_name}
        logger.debug(f'Inserting test_meta: {json.dumps(upload_dict)}')
        return self._perform_insert(target_table='test_meta', dict_to_load=upload_dict, pk_id_col='test_id')
