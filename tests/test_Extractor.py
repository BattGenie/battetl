import os
import re
import pytest
import pandas as pd
from os.path import join
from battetl.extract import Extractor
from battetl import Constants

BASE_DATA_PATH = os.path.join(os.path.dirname(__file__), 'data')
MACCOR_PATH = os.path.join(BASE_DATA_PATH, 'maccor_cycler_data')
MACCOR_SIMPLE_PATH = os.path.join(MACCOR_PATH, 'simple_data')
MACCOR_FRA_PATH = os.path.join(MACCOR_PATH, 'fra_data')
MACCOR_FASTWAVE_PATH = os.path.join(MACCOR_PATH, 'fastwave_data')
MACCOR_TYPE2_PATH = os.path.join(MACCOR_PATH, 'type2_data')
ARBIN_PATH = os.path.join(BASE_DATA_PATH, 'arbin_cycler_data')
ARBIN_SINGLE_PATH = os.path.join(ARBIN_PATH, 'single_data_file')
ARBIN_MULTIPLE_PATH = os.path.join(ARBIN_PATH, 'multiple_data_files')
ARBIN_CAN_PATH = os.path.join(ARBIN_PATH, 'can_data_files')
ARBIN_SIMULATION_PATH = os.path.join(ARBIN_PATH, 'simulation_data_files')


@pytest.mark.extract
def test_bad_path():
    path = join(
        BASE_DATA_PATH,
        'fake/data/path - 079.txt',
    )

    extractor = Extractor()

    with pytest.raises(TypeError):
        df_extracted = extractor.data_from_files(path)


@pytest.mark.extract
@pytest.mark.maccor
def test_extract_maccor_test_data():
    path = join(MACCOR_SIMPLE_PATH, 'BG_Maccor_Testdata - 079.txt')

    extractor = Extractor()
    extractor.data_from_files([path])
    df_extracted = extractor.raw_test_data

    df_loaded = pd.read_csv(
        path,
        sep='\t',
        header=2,
        skipinitialspace=True,
        index_col=False
    )

    assert (df_extracted.equals(df_loaded))
    assert (extractor.cycler_make == 'maccor')


@pytest.mark.extract
@pytest.mark.maccor
def test_extract_maccor_test_data_type2():
    path = join(MACCOR_TYPE2_PATH, 'BG_Maccor_Type2 - 075.txt')

    extractor = Extractor()
    extractor.data_from_files([path])
    df_extracted = extractor.raw_test_data

    df_loaded = pd.read_csv(
        path,
        sep='\t',
        header=3,
        skipinitialspace=True,
        index_col=False
    )

    assert (df_extracted.equals(df_loaded))
    assert (extractor.cycler_make == 'maccor')


@pytest.mark.extract
@pytest.mark.maccor
def test_extract_maccor_stats_data():
    path = join(MACCOR_SIMPLE_PATH, 'BG_Maccor_TestData - 079 [STATS].txt')

    extractor = Extractor()
    extractor.data_from_files([path])
    df_extracted = extractor.raw_cycle_stats

    df_loaded = pd.read_csv(
        path, sep='\t',
        skiprows=8,
        skipinitialspace=True,
        index_col=False)

    assert (df_extracted.equals(df_loaded))
    assert (extractor.cycler_make == 'maccor')


@pytest.mark.extract
@pytest.mark.maccor
def test_extract_maccor_stats_data_pickle():
    path_pkl = join(MACCOR_SIMPLE_PATH, 'BG_Maccor_TestData - 079.pkl')
    path_txt = join(MACCOR_SIMPLE_PATH, 'BG_Maccor_Testdata - 079.txt')

    extractor = Extractor()
    df_extracted = extractor.from_pickle(path_pkl)

    df_loaded = pd.read_csv(
        path_txt,
        sep='\t',
        header=2,
        skipinitialspace=True,
        index_col=False)

    assert (df_extracted.equals(df_loaded))


@pytest.mark.extract
@pytest.mark.arbin
def test_extract_arbin_single_data_file():
    path = join(ARBIN_SINGLE_PATH,
                'BG_Arbin_TestData_Single_File_Channel_26_Wb_1.CSV')

    extractor = Extractor()
    extractor.data_from_files([path])
    df_extracted = extractor.raw_test_data

    df_loaded = pd.read_csv(path, index_col=False)

    assert (df_extracted.equals(df_loaded))
    assert (extractor.cycler_make == 'arbin')


@pytest.mark.extract
@pytest.mark.arbin
def test_extract_arbin_multiple_data_files():
    paths = [
        join(ARBIN_MULTIPLE_PATH,
             'BG_Arbin_TestData_Multiple_Files_Channel_16_Wb_1.CSV'),
        join(ARBIN_MULTIPLE_PATH,
             'BG_Arbin_TestData_Multiple_Files_Channel_16_Wb_2.CSV'),
        join(ARBIN_MULTIPLE_PATH,
             'BG_Arbin_TestData_Multiple_Files_Channel_16_Wb_3.CSV'),
        join(ARBIN_MULTIPLE_PATH,
             'BG_Arbin_TestData_Multiple_Files_Channel_16_Wb_4.CSV')
    ]

    extractor = Extractor()
    extractor.data_from_files(paths)
    df_extracted = extractor.raw_test_data

    df_1 = pd.read_csv(paths[0], index_col=False)
    df_2 = pd.read_csv(paths[1], index_col=False)
    df_3 = pd.read_csv(paths[2], index_col=False)
    df_4 = pd.read_csv(paths[3], index_col=False)
    df_loaded = pd.concat([df_1, df_2, df_3, df_4], ignore_index=True)

    assert (df_extracted.equals(df_loaded))
    assert (extractor.cycler_make == 'arbin')


@pytest.mark.extract
@pytest.mark.arbin
def test_extract_arbin_globalinfo():
    path = join(ARBIN_SINGLE_PATH,
                'BG_Arbin_TestData_Single_File_Channel_26_GlobalInfo.CSV')

    extractor = Extractor()
    extractor.data_from_files([path])

    assert (
        extractor.raw_test_data_meta_data[0] == {
            'Test Name': 'BG_Characterization_25R_Cell_2',
            'Export Time': '06/01/2022 14:47:10',
            'Serial Number': '213079',
            'Channel': '26',
            'Start DateTime': '05/22/2022 12:42:59',
            'Schedule File Name': 'BG\\BG_25R_Characterization+BG_25R.sdx',
            'Creator': 'admin',
            'Comments': '',
            'Software Version': 'Mits8 PV Build: 202110',
            'Schedule Version': 'Schedule Version 8.00.11',
            'MASS(g)': '0',
            'Specific Capacity(Ah/g)': '0',
            'Capacity(Ah)': '2.5',
            'BarCode': '',
            'Has Aux': '512',
            'Has Specail': '0',
            'Log Aux Data Flag': 'True',
            'Log Special Flag': 'False'
        }
    )


@pytest.mark.extract
@pytest.mark.maccor
@pytest.mark.schedules
def test_extract_maccor_simple_procedure():
    procedure_file_name = 'BG_Maccor_Schedule.000'
    path = join(MACCOR_SIMPLE_PATH, procedure_file_name)

    extractor = Extractor()
    extractor.schedule_from_files([path])
    assert (extractor.schedule['file_name'] == path)
    procedure_extracted = extractor.schedule['schedule']

    assert (extractor.cycler_make == Constants.MAKE_MACCOR)

    schedule_check_dict = {
        'step_types': ['Rest', 'Charge', 'Rest',
                       'AdvCycle', 'Dischrge', 'Rest', 'Charge', 'Rest',
                       'AdvCycle', 'Dischrge', 'Rest', 'Charge', 'Rest',
                       'AdvCycle', 'Dischrge', 'Rest', 'Charge', 'Rest',
                       'AdvCycle', 'Dischrge', 'Rest', 'Charge', 'Rest',
                       'AdvCycle', 'Dischrge', 'Rest', 'Charge', 'Rest',
                       'AdvCycle', 'Dischrge', 'Rest', 'Charge', 'Rest',
                       'AdvCycle', 'Dischrge', 'Rest', 'Charge', 'Rest',
                       'End'],
        'chg_steps': [2, 7, 12, 17, 22, 27, 32, 37],
        'dsg_steps': [5, 10, 15, 20, 25, 30, 35],
        "rest_steps": [1, 3, 6, 8, 11, 13, 16, 18, 21, 23, 26, 28, 31, 33, 36, 38]
    }

    # Check that all step types are correctly defined
    for i, step_type in enumerate(schedule_check_dict['step_types']):
        assert (procedure_extracted['MaccorProcedure']['ProcSteps']
                ['TestStep'][i]['StepType'] == step_type)

    assert (extractor.schedule['steps']['chg']
            == schedule_check_dict['chg_steps'])
    assert (extractor.schedule['steps']['dsg']
            == schedule_check_dict['dsg_steps'])
    assert (extractor.schedule['steps']['rst']
            == schedule_check_dict['rest_steps'])


@pytest.mark.extract
@pytest.mark.maccor
@pytest.mark.schedules
def test_extract_maccor_fra_files():
    procedure_file_name = 'BG_EIS.000'
    fra_file_1 = 'BG_EIS_5mV_BaselineCycle.FRA'
    fra_file_2 = 'BG_EIS_10mV_Pot_DQ1.FRA'
    procedure_path = join(MACCOR_FRA_PATH, procedure_file_name)
    fra_path_1 = join(MACCOR_FRA_PATH, fra_file_1)
    fra_path_2 = join(MACCOR_FRA_PATH, fra_file_2)

    extractor = Extractor()
    extractor.schedule_from_files([procedure_path, fra_path_1, fra_path_2])
    assert (extractor.schedule['file_name'] == procedure_path)
    schedule_extracted = extractor.schedule['schedule']

    fra_dict_1 = schedule_extracted['fra_files'][0][fra_file_1]
    fra_dict_2 = schedule_extracted['fra_files'][1][fra_file_2]

    # Check a sample of values.
    assert (fra_dict_1['General']['fratype'] == '3')
    assert (fra_dict_1['Setup']['range'] == '200 mA')
    assert (fra_dict_1['Sweep']['endfreq'] == '.001')

    assert (fra_dict_2['General']['fratype'] == '3')
    assert (fra_dict_2['Setup']['range'] == '200 mA')
    assert (fra_dict_2['Sweep']['endfreq'] == '1')


@pytest.mark.extract
@pytest.mark.maccor
@pytest.mark.schedules
def test_extract_maccor_fastwave_files():
    procedure_file_name = 'BG_ProfileCycling_MP_CVP_MBC2.000'
    fastwave_file_1 = 'MP_CVP_MBC2.MWF'
    fastwave_file_2 = 'other.MWF'
    procedure_path = join(MACCOR_FASTWAVE_PATH, procedure_file_name)
    fastwave_path_1 = join(MACCOR_FASTWAVE_PATH, fastwave_file_1)
    fastwave_path_2 = join(MACCOR_FASTWAVE_PATH, fastwave_file_2)

    extractor = Extractor()
    extractor.schedule_from_files(
        [procedure_path, fastwave_path_1, fastwave_path_2])
    assert (extractor.schedule['file_name'] == procedure_path)
    schedule_extracted = extractor.schedule['schedule']

    fastwave_string_1 = schedule_extracted['fastwave_files'][0][fastwave_file_1]
    fastwave_string_2 = schedule_extracted['fastwave_files'][1][fastwave_file_2]

    # Check the first and last lines
    assert (re.split('\n', fastwave_string_1)[
            0] == 'C\tI\t6.749\tV\t4.2\t1\tV\t>=\t4.2\tT\t1\tA')
    assert (re.split('\n', fastwave_string_1)
            [-1] == 'C\tI\t1.858\tV\t4.2\t3600\tI\t<=\t0.15\tT\t1\tA')

    assert (re.split('\n', fastwave_string_2)[
            0] == 'C\tI\t6.555\tV\t4.2\t1\tV\t>=\t4.2\tT\t1\tA')
    assert (re.split('\n', fastwave_string_2)
            [-1] == 'C\tI\t1.858\tV\t4.2\t3600\tI\t<=\t0.16\tT\t1\tA')


@pytest.mark.extract
@pytest.mark.arbin
@pytest.mark.schedules
def test_extract_arbin_simple_schedule():
    schedule_file_name = 'BG_25R_Characterization+BG_25R.sdx'
    path = join(ARBIN_SINGLE_PATH, schedule_file_name)

    extractor = Extractor()
    extractor.schedule_from_files([path])
    assert (extractor.schedule['file_name'] == path)
    schedule_extracted = extractor.schedule['schedule']

    schedule_check_dict = {
        'step_types': ['Rest', 'Internal Resistance', 'Current(A)', 'Rest'
                       'Set Variable(s)', 'Internal Resistance', 'Current(A)', 'Rest', 'Current(A)', 'Rest',
                       'Set Variable(s)', 'Internal Resistance', 'CCCV', 'Rest', 'Current(A)', 'Rest',
                       'Set Variable(s)', 'Internal Resistance', 'CCCV', 'Rest', 'Current(A)', 'Rest',
                       'Set Variable(s)', 'Internal Resistance', 'CCCV', 'Rest', 'Current(A)', 'Rest',
                       'Set Variable(s)', 'Internal Resistance', 'CCCV', 'Rest', 'Current(A)', 'Rest',
                       'Set Variable(s)', 'Internal Resistance', 'CCCV', 'Rest', 'Current(A)', 'Rest',
                       'Set Variable(s)', 'Internal Resistance', 'CCCV', 'Rest', 'Current(A)', 'Rest',
                       'Set Variable(s)', 'Current(A)', 'Rest'],
        "chg_steps": [7, 13, 19, 25, 31, 37, 43, 48],
        "dsg_steps": [3, 9, 15, 21, 27, 33, 39, 45],
        "rest_steps": [1, 4, 8, 10, 14, 16, 20, 22, 26, 28, 32, 34, 38, 40, 44, 46, 49]
    }

    # Check that all step types were correctly loaded.
    steps = {key: val for key, val in schedule_extracted['ArbinSchedule']['Schedule'].items()
             if key.startswith('step')}
    for i, step in enumerate(steps):
        assert (steps[step]['m_szStepCtrlType'] ==
                schedule_check_dict['step_types'][i])

    assert (extractor.schedule['steps']['chg']
            == schedule_check_dict['chg_steps'])
    assert (extractor.schedule['steps']['dsg']
            == schedule_check_dict['dsg_steps'])
    assert (extractor.schedule['steps']['rst']
            == schedule_check_dict['rest_steps'])


@pytest.mark.extract
@pytest.mark.arbin
@pytest.mark.schedules
def test_extract_arbin_object_file():
    schedule_file_name = 'BG_25R_Characterization+BG_25R.sdx'
    object_file_name = 'BG_25R.to'
    schedule_path = join(ARBIN_SINGLE_PATH, schedule_file_name)
    object_path = join(ARBIN_SINGLE_PATH, object_file_name)

    extractor = Extractor()
    extractor.schedule_from_files([schedule_path, object_path])
    assert (extractor.schedule['file_name'] == schedule_path)
    schedule_extracted = extractor.schedule['schedule']

    object_dict = schedule_extracted['object_file'][object_file_name]['Content']

    assert (object_dict['m_bautocalcncapacity'] == '0')
    assert (object_dict['m_fmass'] == '0')
    assert (object_dict['m_fmaxcurrentcharge'] == '10')
    assert (object_dict['m_fmaxvoltagecharge'] == '4.2')
    assert (object_dict['m_fminvoltagecharge'] == '2.5')
    assert (object_dict['m_fnorminalcapacitor'] == '0')
    assert (object_dict['m_fnorminalcapacity'] == '2.5')
    assert (object_dict['m_fnorminalir'] == '0')
    assert (object_dict['m_fnorminalvoltage'] == '0')
    assert (object_dict['m_fspecificcapacity'] == '0')
    assert (object_dict['ser'] == '508182206')
    assert (object_dict['ver'] == '27265537')


@pytest.mark.extract
@pytest.mark.arbin
@pytest.mark.schedules
def test_extract_arbin_can_bms_file():
    schedule_file_name = 'BG_MBC_Cycling_ZN_TEST+BG_ATL-GC-SDC-2854A6-020H+model_control_channel_29.sdx'
    can_file_name = 'model_control_channel_29.can'
    schedule_path = join(ARBIN_CAN_PATH, schedule_file_name)
    can_path = join(ARBIN_CAN_PATH, can_file_name)

    extractor = Extractor()
    extractor.schedule_from_files([schedule_path, can_path])
    assert (extractor.schedule['file_name'] == schedule_path)
    schedule_extracted = extractor.schedule['schedule']

    can_dict = schedule_extracted['can_bms_file'][can_file_name]['CANConfig']

    # Just spot check a few because the file is big
    assert (can_dict['Creator'] == 'admin')
    assert (can_dict['CANBaudRate'] == '2')
    assert (can_dict['DataEntryItemList']['DataEntryItem']
            [0]['VariableName'] == 'CAN_MV_RX1')


@pytest.mark.extract
@pytest.mark.arbin
@pytest.mark.schedules
def test_extract_arbin_can_formula_file():
    schedule_file_name = 'BG_MBC_Cycling_ZN_TEST+BG_ATL-GC-SDC-2854A6-020H+model_control_channel_29.sdx'
    can_fm_file_name = 'model_control_channel_29.fm'
    schedule_path = join(ARBIN_CAN_PATH, schedule_file_name)
    formula_path = join(ARBIN_CAN_PATH, can_fm_file_name)

    extractor = Extractor()
    extractor.schedule_from_files([schedule_path, formula_path])
    assert (extractor.schedule['file_name'] == schedule_path)
    schedule_extracted = extractor.schedule['schedule']

    can_fm_dict = schedule_extracted['can_formula_file'][can_fm_file_name]

    # Just spot check a few because the file is big
    assert (can_fm_dict['CAN_AuxDataMaxRow']['datamaxrow'] == '0')
    assert (can_fm_dict['BaudRate']['baudrate'] == '2')
    assert (can_fm_dict['CAN_Formalu1']['lableformalu'] == 'F_A')


@pytest.mark.extract
@pytest.mark.arbin
@pytest.mark.schedules
def test_extract_arbin_mapping_file():
    schedule_file_name = 'BG_MBC_Cycling_ZN_TEST+BG_ATL-GC-SDC-2854A6-020H+model_control_channel_29.sdx'
    batch_file_name = 'ArbinSys.bth'
    schedule_path = join(ARBIN_CAN_PATH, schedule_file_name)
    batch_path = join(ARBIN_CAN_PATH, batch_file_name)

    extractor = Extractor()
    extractor.schedule_from_files([schedule_path, batch_path])
    assert (extractor.schedule['file_name'] == schedule_path)
    schedule_extracted = extractor.schedule['schedule']

    batch_dict = schedule_extracted['mapping_file'][batch_file_name]

    # Just spot check a few because the file is big
    assert (batch_dict['Version Section']
            ['current_version'] == 'Map Version 8.00.02')
    assert (batch_dict['Batch_Test29']['m_uglobalcanbmschanidx'] == '-1')
    assert (batch_dict['Batch']['m_szcreator'] == 'admin')


@pytest.mark.extract
@pytest.mark.arbin
@pytest.mark.schedules
def test_extract_arbin_simulation_files():
    schedule_file_name = 'BG_MBC_eta0p025_25R+BG_25R.sdx'
    sim_file_1 = 'MWT_MBC_eta0p025.txt'
    sim_file_2 = 'sim_file_2.txt'
    schedule_path = join(ARBIN_SIMULATION_PATH, schedule_file_name)
    sim_path_1 = join(ARBIN_SIMULATION_PATH, sim_file_1)
    sim_path_2 = join(ARBIN_SIMULATION_PATH, sim_file_2)

    extractor = Extractor()
    extractor.schedule_from_files([schedule_path, sim_path_1, sim_path_2])
    assert (extractor.schedule['file_name'] == schedule_path)
    schedule_extracted = extractor.schedule['schedule']

    sim_string_1 = schedule_extracted['simulation_files'][0][sim_file_1]
    sim_string_2 = schedule_extracted['simulation_files'][1][sim_file_2]

    # Check the first and last lines in the simulation files
    assert (re.split('\n', sim_string_1)[0] == '1\t7.5')
    assert (re.split('\n', sim_string_1)[-1] == '2059\t0.1')

    assert (re.split('\n', sim_string_2)[0] == '1\t8.5')
    assert (re.split('\n', sim_string_2)[-1] == '2059\t0.2')
