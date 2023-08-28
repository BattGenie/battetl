import os
import re
import json
import xmltodict
import configparser
import pandas as pd
from collections import OrderedDict

from battetl import logger, Constants, Utils
from battetl.utils import DashOrderedDict


class Extractor:
    def __init__(self):
        """
        An interface to extract battery test data from raw data files. 
        """
        self.raw_test_data_meta_data = []
        self.raw_cycle_stats_meta_data = []
        self.raw_test_data = pd.DataFrame(dtype=object)
        self.raw_cycle_stats = pd.DataFrame(dtype=object)
        self.cycler_make = ''

        self.schedule = {
            'schedule': DashOrderedDict(),
            'file_name': '',
            'steps': {'chg': [], 'dsg': [], 'rst': []}
        }

    def data_from_files(self, paths: list[str], file_meta: dict = None) -> pd.DataFrame:
        """
        Extracts multiple test data files into a single pandas DataFrame.
        If only a single data file exists then it only extracts that data.

        Parameters
        ----------
        paths : list[str]
            Relative or absolute paths to the target data files.
        file_meta : dict, optional
            Dictionary containing the user defined column names for the test data. The default is None.
        Returns
        -------
        pd.DataFrame
            A pandas DataFrame containing the test data.
        """
        if type(paths) != list:
            raise TypeError('Input paths is not list')

        logger.info(f'Total {len(paths)} files')

        for path in paths:
            if file_meta:
                self.__unstructured_data_from_file(
                    path=path,
                    file_meta=file_meta)
            else:
                self.__data_from_file(path=path)

        logger.info('Extract success')

    def schedule_from_files(self, paths: list[str]) -> dict:
        """
        Reads Arbin schedules and associated files or Maccor procedures and associated 
        files from the passed paths list and saves them together in a nested dictionary.

        Parameters
        ----------
        paths : list[str]
            Relative or absolute paths to the Maccor schedule and associated files
        """
        if type(paths) != list:
            raise TypeError('Input paths is not list')

        logger.info(f'Total {len(paths)} files')

        for i, path in enumerate(paths):
            if path.endswith('.000'):
                logger.info("Processing procedure files for Maccor")
                self.schedule['schedule'] = self.__maccor_procedure_from_files(
                    paths)
                break
            elif path.endswith('.sdu') or path.endswith('.sdx'):
                logger.info("Processing schedule files for Arbin")
                self.schedule['schedule'] = self.__arbin_schedule_from_files(
                    paths)

                break
            elif i == (len(paths)-1):
                logger.error(
                    'Unable to identify schedule type from paths: ' + str(paths))

        return self.schedule

    def schedule_from_files(self, paths: list[str]) -> dict:
        """
        Reads Arbin schedules and associated files or Maccor procedures and associated 
        files from the passed paths list and saves them together in a nested dictionary.

        Parameters
        ----------
        paths : list[str]
            Relative or absolute paths to the Maccor schedule and associated files
        """
        if type(paths) != list:
            raise TypeError('Input paths is not list')

        logger.info(f'Total {len(paths)} files')

        for i, path in enumerate(paths):
            if path.endswith('.000'):
                logger.info("Processing procedure files for Maccor")
                self.schedule['schedule'] = self.__maccor_procedure_from_files(
                    paths)
                break
            elif path.endswith('.sdu') or path.endswith('.sdx'):
                logger.info("Processing schedule files for Arbin")
                self.schedule['schedule'] = self.__arbin_schedule_from_files(
                    paths)

                break
            elif i == (len(paths)-1):
                logger.error(
                    'Unable to identify schedule type from paths: ' + str(paths))

        return self.schedule

    def __unstructured_data_from_file(self, path: str, file_meta: dict) -> pd.DataFrame:
        """
        Reads unstructured data from the passed file path and returns it as a pandas DataFrame.
        Parameters
        ----------
        path : str
            Relative or absolute path to the datafile.
        file_meta : dict
            Dictionary containing the meta data for the file.
        Returns
        -------
        df : pandas.DataFrame
            A pandas DataFrame containing the data file.
        """
        logger.info(f'Load file path: {path}')
        logger.info(f'Unstructured data from file. File meta: {file_meta}')
        if not os.path.exists(path):
            raise FileNotFoundError(f'Unable to load file {path}')

        # Check file_meta contains all required keys
        Utils.validate_file_meta(file_meta)

        df = pd.read_csv(path, **file_meta['pandas_read_csv_args'])

        self.raw_test_data = pd.concat(
            [
                self.raw_test_data,
                df
            ],
            ignore_index=True
        )
        logger.debug(
            f'Update raw_test_data. Total rows: {self.raw_test_data.shape[0]}')

        return df

    def __maccor_procedure_from_files(self, paths: list[str]) -> DashOrderedDict:
        """
        Reads the Maccor procedure and associated files from the passed file path list.
        Returns all files a dictionaries in a single nested DashOrderedDict. Supported files to 
        include in paths list are Maccor procedures (.000) (required),  FastWave (*.MWF)  
        and frequency response analyzer (*.FRA) files.

        Parameters
        ----------
        paths : list[str]
            Relative or absolute paths to the Maccor schedule and associated files

        Returns
        -------
        procedure : DashOrderedDict
            The Maccor procedure and associated files saved as a nested dict.
        """
        procedure_dod = DashOrderedDict()
        fra_dicts = []
        fastwave_dicts = []

        if self.cycler_make == '':
            self.cycler_make = Constants.MAKE_MACCOR
        elif self.cycler_make != Constants.MAKE_MACCOR:
            logger.error(
                "Passed schedule does not match previously assigned cycler_make!")
            return {}

        for path in paths:
            if path.endswith('.000'):
                if not self.schedule['file_name']:
                    self.schedule['file_name'] = path
                    logger.info(
                        f'Importing Maccor procedure {path}')
                    procedure = self.__procedure_from_file(path)
                else:
                    logger.error("A schedule file name is already defined!")
            elif path.endswith('.FRA'):
                logger.info(
                    f'Importing FRA file {path}')
                fra_dicts.append(self.__fra_from_file(path))
            elif path.endswith('.MWF'):
                logger.info(
                    f'Importing fastwave file {path}')
                fastwave_dicts.append(self.__fastwave_from_file(path))
            else:
                logger.warning(f'Unknown file type included: {path}')

        try:
            procedure_dod.set('MaccorProcedure', procedure)
        except NameError:
            logger.error(
                "Missing Maccor procedure file from included paths! " + str(paths))
            return {}

        self.schedule['steps'] = self.__steps_from_procedure(procedure)

        if fastwave_dicts:
            procedure_dod.set('fastwave_files', fastwave_dicts)
        if fra_dicts:
            procedure_dod.set('fra_files', fra_dicts)

        return procedure_dod

    def __procedure_from_file(self, path) -> dict:
        """
        Reads the Maccor procedure file (*.000) and returns the contents 
        as a nested dict. This function was pulled from BEEP:
        https://github.com/TRI-AMDD/beep/blob/master/beep/protocol/maccor.py

        Parameters
        ----------
        path : str
            Relative or absolute path to the procedure file.

        Returns
        -------
        procedure : dict
            The Maccor procedure as a nested dictionary.
        """
        with open(path, "rb") as f:
            text = f.read().decode(Constants.MACCOR_PROCEDURE_FILE_ENCODING)
        procedure = xmltodict.parse(
            text, process_namespaces=False, strip_whitespace=True)
        return procedure['MaccorTestProcedure']

    def __fra_from_file(self, path) -> dict:
        """
        Reads the Maccor frequency response analyzer file (*.FRA) and returns the contents 
        as a nested dictionary where the highest level key is the file name. 

        Parameters
        ----------
        path : str
            Relative or absolute path to the FRA file.

        Returns
        -------
        fra_dict : dict
            The Maccor FRA file returned as a nested dictionary. 
        """
        parser = configparser.ConfigParser()
        parser.read(path)
        fra_dict = {section: dict(parser.items(section))
                    for section in parser.sections()}

        return {os.path.split(path)[-1]: fra_dict}

    def __fastwave_from_file(self, path) -> dict:
        """
        Reads the Maccor fastwave file (*.MFW) and returns the contents 
        as a nested dictionary where the highest level key is the file name
        and the corresponding value is the file contents as a string.

        Parameters
        ----------
        path : str
            Relative or absolute path to the fastwave file.

        Returns
        -------
        fra_dict : dict
            The Maccor fastwave file returned as a nested dictionary. 
        """
        with open(path, 'r') as file:
            fastwave_string = file.read()
        return {os.path.split(path)[-1]: fastwave_string}

    def __steps_from_procedure(self, procedure: dict) -> dict:
        """
        Determines which steps are charge/discharge/rest from the passed Maccor procedure
        and returns them as a dictionary of lists.

        Parameters
        ----------
        schedule : dict
            A nested dictionary containing the Maccor procedure. Assumes a layout generated
            by `__procedure_from_file()`

        Returns
        -------
        steps : dict
            A dict containing lists of the charge (key->'chg'), discharge, (key->'dsg'), and 
            rest (key->'rst') steps.
        """
        chg_steps = []
        dsg_steps = []
        rest_steps = []

        step_dict_list = procedure['ProcSteps']['TestStep']
        for i, step in enumerate(step_dict_list):
            if step['StepType'] in Constants.MACCOR_CHARGE_STEP_NAMES:
                chg_steps.append(i+1)
            elif step['StepType'] in Constants.MACCOR_DISCHARGE_STEP_NAMES:
                dsg_steps.append(i+1)
            elif step['StepType'] == 'Rest':
                rest_steps.append(i+1)

        steps = {'chg': chg_steps, 'dsg': dsg_steps, 'rst': rest_steps}
        logger.info("Identified Maccor procedure steps: " + json.dumps(steps))

        return steps

    def __arbin_schedule_from_files(self, paths) -> DashOrderedDict:
        """
        Reads the Arbin procedure and associated files the passed file path list.
        Returns all files as dictionaries in a single nested dictionary. Supported files to
        include in the paths list are Arbin schedules (*.sdx or *.sdu) (required),
        object files (*.to), simulation files (*.txt), CAN BMS files (*.can), and mapping 
        files (*.bth).

        Parameters
        ----------
        paths : list[str]
            Relative or absolute path to the schedule and associated files. 

        Returns
        -------
        procedure : DashOrderedDict
            The Maccor procedure and associated files saved as a nested dict.
        """
        schedule_dod = DashOrderedDict()
        object_file_dict = {}
        can_bms_file_dict = {}
        can_fm_file_dict = {}
        mapping_file_dict = {}
        simulation_dicts = []

        if self.cycler_make == '':
            self.cycler_make = Constants.MAKE_ARBIN
        elif self.cycler_make != Constants.MAKE_ARBIN:
            logger.error(
                "Passed schedule does not match previously assigned cycler_make!")
            return {}

        for path in paths:
            if path.endswith('.sdx') or path.endswith('.sdu'):
                if not self.schedule['file_name']:
                    self.schedule['file_name'] = path
                    logger.info("Importing Arbin schedule " +
                                self.schedule['file_name'])
                    schedule = self.__schedule_from_file(path)
                else:
                    logger.error("A schedule file is already defined!")
            elif path.endswith('.to'):
                if not object_file_dict:
                    logger.info("Importing Arbin object file " +
                                path)
                    object_file_dict = self.__object_from_file(path)
                else:
                    logger.error("An object file is already defined!")
            elif path.endswith('.can'):
                if not can_bms_file_dict:
                    logger.info("Importing Arbin CAN BMS file " +
                                path)
                    can_bms_file_dict = self.__can_bms_from_file(path)
                else:
                    logger.error("A CAN BMS file is already defined")
            elif path.endswith('.fm'):
                if not can_fm_file_dict:
                    logger.info("Importing Arbin CAN formula file " +
                                path)
                    can_fm_file_dict = self.__can_fm_from_file(path)
                else:
                    logger.error("A CAN formula file is already defined")
            elif path.endswith('.bth'):
                if not mapping_file_dict:
                    logger.info("Importing Arbin mapping file " +
                                path)
                    mapping_file_dict = self.__mapping_from_file(path)
                else:
                    logger.error("A mapping file is already defined")
            elif path.endswith('.txt'):
                logger.info("Importing Arbin simulation file " +
                            path)
                simulation_dicts.append(self.__simulation_from_file(path))
            else:
                logger.warning("Unknown file type included: " + str(path))

        try:
            schedule_dod.set('ArbinSchedule', schedule)
        except NameError:
            logger.error(
                "Missing Arbin schedule file from included paths!" + paths)
            return {}

        self.schedule['steps'] = self.__steps_from_schedule(schedule)

        if object_file_dict:
            schedule_dod.set('object_file', object_file_dict)
        if can_bms_file_dict:
            schedule_dod.set('can_bms_file', can_bms_file_dict)
        if can_fm_file_dict:
            schedule_dod.set('can_formula_file', can_fm_file_dict)
        if mapping_file_dict:
            schedule_dod.set('mapping_file', mapping_file_dict)
        if simulation_dicts:
            schedule_dod.set('simulation_files', simulation_dicts)

        return schedule_dod

    def __schedule_from_file(self, path) -> DashOrderedDict:
        """
        Reads the Arbin schedule (*.sdu or *.sdx) file and returns the contents 
        as a nested DashOrderedDict. This function was pulled from BEEP:
        https://github.com/TRI-AMDD/beep/blob/master/beep/protocol/arbin.py

        Parameters
        ----------
        path : str
            Relative or absolute path to the schedule file.

        Returns
        -------
        schedule : DashOrderedDict
            The Arbin schedule as a nested dictionary.
        """
        schedule = DashOrderedDict()

        with open(path, "rb") as f:
            text = f.read()
        text = text.decode(Constants.ARBIN_SCHEDULE_FILE_ENCODING)
        split_text = re.split(r"\[(.+)\]", text)

        for heading, body in zip(split_text[1::2], split_text[2::2]):
            body_lines = re.split(r"[\r\n]+", body.strip())
            body_dict = OrderedDict([line.split("=", 1)
                                    for line in body_lines])
            heading = heading.replace("_", ".")
            schedule.set(heading, body_dict)

        return schedule

    def __object_from_file(self, path) -> dict:
        """
        Reads the Arbin object file (*.to) and returns the contents 
        as a nested dictionary where top level key is the file name.

        Parameters
        ----------
        path : str
            Relative or absolute path to the object file.

        Returns
        -------
        object_file_dict : dict
            The Arbin object file returned as a dictionary. 
        """
        parser = configparser.ConfigParser()
        parser.read(path)
        confdict = {section: dict(parser.items(section))
                    for section in parser.sections()}
        return {os.path.split(path)[-1]: confdict}

    def __can_bms_from_file(self, path) -> OrderedDict:
        """
        Reads the Arbin CAN BMS file (*.to) and returns the contents 
        as a nested dictionary.

        Parameters
        ----------
        path : str
            Relative or absolute path to the CAN BMS file.

        Returns
        -------
        can_bms_dict: OrderedDict
            The CAN BMS file returned as a dictionary. 
        """
        with open(path, "rb") as f:
            text = f.read().decode('UTF-8')
            can_dict = xmltodict.parse(
                text, process_namespaces=False, strip_whitespace=True)
        return {os.path.split(path)[-1]: can_dict}

    def __can_fm_from_file(self, path) -> OrderedDict:
        """
        Reads the Arbin CAN formula file (*.fm) and returns the contents 
        as a nested dictionary where top level key is the file name.

        Parameters
        ----------
        path : str
            Relative or absolute path to the CAN formula file.

        Returns
        -------
        can_bms_dict: OrderedDict
            The CAN formula file returned as a dictionary. 
        """
        parser = configparser.ConfigParser()
        parser.read(path)
        fmdict = {section: dict(parser.items(section))
                  for section in parser.sections()}
        return {os.path.split(path)[-1]: fmdict}

    def __mapping_from_file(self, path) -> dict:
        """
        Reads the Arbin mapping file file (*.bth) and returns the contents 
        as a nested dictionary where top level key is the file name.

        Parameters
        ----------
        path : str
            Relative or absolute path to the mapping file.

        Returns
        -------
        mapping_file_dict: dict
            The mapping file returned as a dictionary. 
        """
        parser = configparser.ConfigParser()
        # Note: This is not the exact encoding and some special characters are being read incorrectly, but most of the file is being read correctly.
        parser.read(path, encoding='ISO-8859-1')
        mapping_dict = {section: dict(parser.items(section))
                        for section in parser.sections()}
        return {os.path.split(path)[-1]: mapping_dict}

    def __simulation_from_file(self, path) -> dict:
        """
        Reads the Arbin simulation file (*.MFW) and returns the contents 
        as a dictionary where the key is the file name and the corresponding
        value is the file contents as a string. 

        Parameters
        ----------
        path : str
            Relative or absolute path to the simulation file.

        Returns
        -------
        simulation_dict : dict
            The Arbin simulation file returned as a nested dictionary. 
        """
        with open(path, 'r') as file:
            simulation_string = file.read()
        return {os.path.split(path)[-1]: simulation_string}

    def __steps_from_schedule(self, schedule: DashOrderedDict) -> dict:
        """
        Determines which steps are charge/discharge/rest from the passed Arbin schedule
        and returns them as a dictionary of lists.

        Parameters
        ----------
        schedule : DashOrderedDict
            A nested DashOrderedDict containing the Arbin schedule. Assumes a layout generated
            by `__schedule_from_file()`

        Returns
        -------
        steps : dict
            A dict containing lists of the charge (key->'chg'), discharge, (key->'dsg'), and 
            rest (key->'rst') steps.
        """
        chg_steps = []
        dsg_steps = []
        rest_steps = []

        step_dicts = {
            key: val for key, val in schedule['Schedule'].items() if key.startswith('Step')}

        for i, step in enumerate(step_dicts):
            step_num = i+1
            step_type = step_dicts[step]['m_szStepCtrlType']
            if step_type == 'Current(A)':
                ctrl_value = step_dicts[step]['m_szCtrlValue']
                if ctrl_value[0] == '-':
                    logger.info("Categorizing 'Current(A)' control value " +
                                ctrl_value+" as discharge, step " + str(step_num))
                    dsg_steps.append(step_num)
                else:
                    logger.info("Categorizing 'Current(A)' control value " +
                                ctrl_value+" as charge, step " + str(step_num))
                    chg_steps.append(step_num)
            elif step_type == 'Current Ramp(A)':
                ctrl_value = step_dicts[step]['m_szCtrlValue']
                if ctrl_value[0] == '-':
                    logger.info("Categorizing 'Current Ramp(A)' control value " +
                                ctrl_value+" as discharge, step " + str(step_num))
                    dsg_steps.append(step_num)
                else:
                    logger.info("Categorizing 'Current Ramp(A)' control value " +
                                ctrl_value+" as charge, step " + str(step_num))
                    chg_steps.append(step_num)
            elif step_type == 'CCCV':
                logger.info(
                    "Categorizing 'CCCV' as charge, step " + str(step_num))
                chg_steps.append(step_num)
            elif step_type == 'CV':
                logger.info(
                    "Categorizing 'CV' as charge, step " + str(step_num))
                chg_steps.append(step_num)
            elif step_type == 'Current Simulation':
                logger.info("Categorizing 'Current Simulation' control value " +
                            ctrl_value+" as charge, step " + str(step_num))
                chg_steps.append(step_num)
            elif step_type == 'Rest':
                logger.info(
                    "Categorizing 'Rest' as rest, step " + str(step_num))
                rest_steps.append(step_num)

        steps = {'chg': chg_steps, 'dsg': dsg_steps, 'rst': rest_steps}
        logger.info("Identified Maccor procedure steps: " + json.dumps(steps))

        return steps

    def __data_from_file(self, path: str) -> pd.DataFrame:
        """
        Reads data from the passed file path and returns it as a pandas DataFrame.

        Parameters
        ----------
        path : str
            Relative or absolute path to the datafile. 

        Returns
        -------
        df : pandas.DataFrame
            A pandas DataFrame containing the data file.
        """
        logger.info(f'Load file path: {path}')
        if not os.path.exists(path):
            raise FileNotFoundError(f'Unable to load file {path}')

        headerLines, headerInfo = self.__get_header_lines(path)
        logger.debug(f'header lines: {headerLines}')
        logger.debug(f'header info: {headerInfo}')

        # Arbin Global Info
        if headerLines == -1:
            logger.debug('Arbin Global Info')
            self.raw_test_data_meta_data.append(headerInfo)
            logger.debug('Update raw_test_data_meta_data')
            self.cycler_make = Constants.MAKE_ARBIN
            return pd.DataFrame()

        df = pd.read_csv(
            path,
            skiprows=headerLines,
            sep='\t' if headerLines > 0 else ',',
            skipinitialspace=True,
            index_col=False,
            encoding_errors='replace'
        )

        logger.debug(
            f'Read {df.shape[0]} rows and {df.shape[1]} columns from {path}')

        df.columns = df.columns.str.strip()

        cycleMake, dataType = Utils.get_cycle_make(df.columns)
        logger.info(f'Cycle make: {cycleMake}. Data type: {dataType}')

        if cycleMake and dataType:
            self.cycler_make = cycleMake

            # meta data
            if cycleMake == Constants.MAKE_MACCOR:
                if dataType == Constants.DATA_TYPE_TEST_DATA:
                    self.raw_test_data_meta_data.append(headerInfo)
                    logger.debug('Update raw_test_data_meta_data')
                if dataType == Constants.DATA_TYPE_CYCLE_STATS:
                    self.raw_cycle_stats_meta_data.append(headerInfo)
                    logger.debug('Update raw_cycle_stats_meta_data')

            # test data
            if dataType == Constants.DATA_TYPE_TEST_DATA:
                self.raw_test_data = pd.concat(
                    [
                        self.raw_test_data,
                        df
                    ],
                    ignore_index=True
                )
                logger.debug(
                    f'Update raw_test_data. Total rows: {self.raw_test_data.shape[0]}')

            if dataType == Constants.DATA_TYPE_CYCLE_STATS:
                self.raw_cycle_stats = pd.concat(
                    [
                        self.raw_cycle_stats,
                        df
                    ],
                    ignore_index=True
                )
                logger.debug(
                    f'Update raw_cycle_stats. Total rows: {self.raw_cycle_stats.shape[0]}')

        return df

    def __get_header_lines(self, path: str) -> tuple[int, dict]:
        """
        Calculate header lines and extract info

        Parameters
        ----------
            path : str
                Test data file path

        Returns
        -------
            count : int
                Header lines
            header : int
                Header info
        """

        file = open(path, 'r', errors='replace')
        count = 0
        header = {}

        while True:
            line = file.readline()

            # The first line of the Arbin GlobalInfo file will be "TEST REPORT"
            if 'test report' in line.lower():
                file.close()
                return -1, self.__read_global_info(path)

            line = line.replace('\n', '').split('\t')
            line = list(filter(None, line))

            lineLen = len(line)
            if lineLen == 2 or lineLen == 0 or line[0][-1] == ':' or line in [
                ['Charge'], ['Discharge']
            ]:
                count += 1

                # Extract header info
                if lineLen == 2:
                    key = line[0][:-2] if line[0][-1] == ':' else line[0]
                    value = line[1]
                    header[key] = value
            else:
                break

        file.close()
        return count, header

    def __read_global_info(self, path):
        """
        Method for Arbin GlobalInfo file

        Parameters
        ----------
            path : str
                GlobalInfo data file path

        Returns
        -------
            header : int
                Header info
        """
        logger.debug('Read GlobalInfo')
        header = {}
        file = open(path, 'r')
        lines = []
        while True:
            line = file.readline()
            line = line.replace('\n', '').split(',')
            if line[0] == '':
                line = list(filter(None, line))
            lines.append(line)

            if not line:
                break

        file.close()

        # Hard-coding for the current situation
        # Test Name
        header[lines[1][0].strip()] = lines[1][1].strip()
        # Export Time
        header[lines[2][0].strip()] = lines[2][1].strip()
        # Serial Number
        header[lines[1][2].strip()] = lines[2][2].strip()
        # Other Info
        for i in range(len(lines[3])):
            header[lines[3][i].strip()] = lines[4][i].strip()

        return header

    def from_pickle(self, path) -> pd.DataFrame:
        """
        Reads data from the passed file path and returns it as a pandas DataFrame.

        Parameters
        ----------
        path : str
            Relative or absolute path to the datafile. 

        Returns
        -------
        df : pandas.DataFrame
            A pandas DataFrame containing the data file.
        """
        logger.info(f'Load pickle path: {path}')
        if not os.path.exists(path):
            raise FileNotFoundError(f'Unable to load file {path}')

        df = pd.read_pickle(path)
        logger.debug(
            f'Read {df.shape[0]} rows and {df.shape[1]} columns from {path}')

        return df
