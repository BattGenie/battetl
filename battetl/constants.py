class Constants:
    DEFAULT_TIME_ZONE = 'America/Los_Angeles'

    BATTDB_SCHEMA_VERSION = 11.1
    BATTDB_QUICK_SCHEMA_VERSION = 1.1

    DATABASE_MAX_RETRIES = 10
    DATABASE_RETRY_DELAY = 10
    DATABASE_MAX_RETRY_DELAY = 60

    MAKE_ARBIN = 'arbin'
    MAKE_MACCOR = 'maccor'
    DATA_TYPE_TEST_DATA = 'test_data'
    DATA_TYPE_CYCLE_STATS = 'cycle_stats'

    MACCOR_PROCEDURE_FILE_ENCODING = 'UTF-8'

    MACCOR_CHARGE_STEP_NAMES = ['Charge', 'Chg Func', 'FastWave']
    # Yes, discharge is actually misspelled in the extract procedure files
    MACCOR_DISCHARGE_STEP_NAMES = ['Dischrge', 'Dis Func']

    ARBIN_SCHEDULE_FILE_ENCODING = 'latin-1'

    PREFIX_ARBIN_THERMOCOUPLE = 'aux_temperature_'
    PREFIX_MACCOR_THERMOCOUPLE = 'temp '
    TEMPLATE_RENAMED_THERMOCOUPLE = 'thermocouple_X_c'

    BATTVIZ_TEST_DATA_PATH = 'd/Test-Data/test-data'
    BATTVIZ_CYCLE_STATS_PATH = 'd/cycling-results/cycling-results'

    COLUMNS_TEST_DATA = {
        'test_data_id',
        'test_id',
        'cycle',
        'step',
        'test_time_s',
        'step_time_s',
        'current_ma',
        'voltage_mv',
        'recorded_datetime',
        'unixtime_s',
        'thermocouple_temps_c',
        'other_details',
    }
    COLUMNS_CYCLE_STATS = {
        'cycle_stats_id',
        'test_id',
        'cycle',
        'test_time_s',
        'reported_charge_capacity_mah',
        'reported_discharge_capacity_mah',
        'reported_charge_energy_mwh',
        'reported_discharge_energy_mwh',
        'reported_charge_time_s',
        'reported_discharge_time_s',
        'calculated_charge_capacity_mah',
        'calculated_max_charge_temp_c',
        'calculated_discharge_capacity_mah',
        'calculated_max_discharge_temp_c',
        'calculated_cc_charge_time_s',
        'calculated_cv_charge_time_s',
        'calculated_cc_capacity_mah',
        'calculated_cv_capacity_mah',
        'reported_coulombic_efficiency',
        'calculated_coulombic_efficiency',
        'calculated_fifty_percent_charge_time_s',
        'calculated_eighty_percent_charge_time_s',
        'calculated_charge_energy_mwh',
        'calculated_discharge_energy_mwh',
        'other_details',
    }
    COLUMNS_ARBIN_TEST_DATA_ONLY = {
        'Date Time',
        'ACR (Ohm)',
        'dQ/dV (Ah/V)',
        'Internal Resistance (Ohm)',
        'dV/dQ (V/Ah)',
        'dV/dt (V/s)',
        'Data Point',
    }
    COLUMNS_ARBIN_CYCLE_STATS_ONLY = {
        'Charge Time (s)',
        'Date_Time',
        'mAh/g',
        'Coulombic Efficiency (%)',
        'V_Max_On_Cycle (V)',
        'Discharge Time (s)',
    }
    COLUMNS_MACCOR_TEST_DATA_ONLY = {
        'Cyc#',
        'StepTime(s)',
        'DPt Time',
        'Current(A)',
        'Capacity(Ah)',
        'Step',
        'EV Temp',
        'Voltage(V)',
        'TestTime(s)',
        'Temp 1',
    }

    COLUMNS_MACCOR_TEST_DATA_TYPE2_ONLY = {
        'Rec',
        'Cycle P',
        'Cycle C',
        'Capacity',
        'Energy',
        'MD',
        'ES',
        'DPT Time',
    }

    COLUMNS_MACCOR_CYCLE_STATS_ONLY = {
        'T1_End',
        'T1_Max',
        'T1_Start',
        'T1_Min',
        'Cycle',
        'Date',
        'AH-OUT',
        'AH-IN',
    }

    COLUMNS_MACCOR_CYCLE_STATS_CUSTOMER1 = {
        'Cycle',
        'AH-IN',
        'AH-OUT',
        'T1_Start',
        'T1_End',
        'T1_Min',
        'T1_Max',
        'T1_Start',
        'T1_End',
        'T1_Min',
        'T1_Max',
        'Date',
    }

    COLUMNS_MACCOR_TEST_DATA_CUSTOMER1 = {
        'Cyc#',
        'Step',
        'TestTime(s)',
        'StepTime(s)',
        'Capacity(Ah)',
        'Watt-hr',
        'Current(A)'
        'Voltage(V)',
        'ES',
        'DPt Time',
        'Volt 1',
        'ManufacturerAccess (0x00)',
        'AtRate (0x02)',
        'AtRateTimeToEmpty (0x04)',
        'Temperature (0x06)',
        'Voltage (0x08)',
        'BatteryStatus (0x0A)',
        'Current (0x0C)',
        'RemainingCapacity (0x10)',
        'FullChargeCapacity (0x12)',
        'AverageCurrent (0x14)',
        'AverageTimeToEmpty (0x16)',
        'AverageTimeToFull (0x18)',
        'RelativeStateOfCharge (0x2C)',
        'ChargingVoltage (0x30)',
        'ChargingCurrent (0x32)',
        'DesignCapacity (0x3C)',
    }

    COLUMNS_TO_MILLI = {
        # Test Data
        'voltage_v': 'voltage_mv',
        'current_a': 'current_ma',
        'voltage': 'voltage_mv',
        'current': 'current_ma',
        'charge_capacity_ah': 'charge_capacity_mah',
        'discharge_capacity_ah': 'discharge_capacity_mah',
        'charge_energy_wh': 'charge_energy_mwh',
        'discharge_energy_wh': 'discharge_energy_mwh',
        'power_w': 'power_mw',
        'impedance_ohm': 'impedance_mohm',
        'capacity_ah': 'capacity_mah',
        # Arbin Test Data
        'arbin_charge_capacity_ah': 'arbin_charge_capacity_mah',
        'arbin_discharge_capacity_ah': 'arbin_discharge_capacity_mah',
        'arbin_charge_energy_wh': 'arbin_charge_energy_mwh',
        'arbin_discharge_energy_wh': 'arbin_discharge_energy_mwh',
        # Maccor Test Data
        'maccor_capacity_ah': 'maccor_capacity_mah',
        'maccor_energy_wh': 'maccor_energy_mwh',
        'capacity': 'maccor_capacity_mah',
        'energy': 'maccor_energy_mwh',
        # Arbin Cycle Stats
        'reported_charge_capacity_ah': 'reported_charge_capacity_mah',
        'reported_discharge_capacity_ah': 'reported_discharge_capacity_mah',
        'reported_charge_energy_wh': 'reported_charge_energy_mwh',
        'reported_discharge_energy_wh': 'reported_discharge_energy_mwh',
    }

    COLUMNS_MAPPING_ARBIN_TEST_DATA = {
        'Date_Time': 'recorded_datetime',
        'Internal Resistance (Ohm)': 'impedance_ohm',
        'Date_Time': 'recorded_datetime',
        'Date Time': 'recorded_datetime',
        'Data Point': 'data_point',
        'Test Time (s)': 'test_time_s',
        'Test Time(s)': 'test_time_s',
        'Test_Time(s)': 'test_time_s',
        'Step Time (s)': 'step_time_s',
        'Step_Time(s)': 'step_time_s',
        'Step Time(s)': 'step_time_s',
        'Cycle Index': 'cycle',
        'Cycle_Index': 'cycle',
        'Step Index': 'step',
        'Step_Index': 'step',
        'TC_Counter1': 'tc_counter1',
        'Voltage (V)': 'voltage_v',
        'Voltage(V)': 'voltage_v',
        'Current (A)': 'current_a',
        'Current(A)': 'current_a',
        'Charge Capacity (Ah)': 'arbin_charge_capacity_ah',
        'Charge Capacity(Ah)': 'arbin_charge_capacity_ah',
        'Charge_Capacity(Ah)': 'arbin_charge_capacity_ah',
        'Discharge Capacity (Ah)': 'arbin_discharge_capacity_ah',
        'Discharge Capacity(Ah)': 'arbin_discharge_capacity_ah',
        'Discharge_Capacity(Ah)': 'arbin_discharge_capacity_ah',
        'Charge Energy (Wh)': 'arbin_charge_energy_wh',
        'Charge_Energy (Wh)': 'arbin_charge_energy_wh',
        'Charge_Energy(Wh)': 'arbin_charge_energy_wh',
        'Discharge Energy (Wh)': 'arbin_discharge_energy_wh',
        'Discharge Energy(Wh)': 'arbin_discharge_energy_wh',
        'Discharge_Energy(Wh)': 'arbin_discharge_energy_wh',
        'Power (W)': 'power_w',
    }
    COLUMNS_MAPPING_ARBIN_CYCLE_STATS = {
        'Date_Time': 'recorded_datetime',
        'Test Time (s)': 'test_time_s',
        'Test Time(s)': 'test_time_s',
        'Step Time (s)': 'step_time_s',
        'Step Time(s)': 'step_time_s',
        'Cycle Index': 'cycle',
        'Step Index': 'step',
        'TC_Counter1': 'tc_counter1',
        'Voltage(V)': 'voltage_v',
        'Voltage (V)': 'voltage_v',
        'Current (A)': 'current_a',
        'Current(A)': 'current_a',
        'Charge Capacity (Ah)': 'reported_charge_capacity_ah',
        'Charge Capacity(Ah)': 'reported_charge_capacity_ah',
        'Discharge Capacity (Ah)': 'reported_discharge_capacity_ah',
        'Discharge Capacity(Ah)': 'reported_discharge_capacity_ah',
        'Charge Time (s)': 'reported_charge_time_s',
        'Charge Time(s)': 'reported_charge_time_s',
        'Discharge Time (s)': 'reported_discharge_time_s',
        'Coulombic Efficiency (%)': 'reported_coulombic_efficiency',
        'Charge Energy (Wh)': 'reported_charge_energy_wh',
        'Charge_Energy(Wh)': 'reported_charge_energy_wh',
        'Discharge Energy (Wh)': 'reported_discharge_energy_wh',
        'Discharge Energy(Wh)': 'reported_discharge_energy_wh',
    }
    COLUMNS_MAPPING_MACCOR_TEST_DATA = {
        'Cyc#': 'cycle',
        'Cycle P': 'cycle',
        'Step': 'step',
        'TestTime(s)': 'test_time_s',
        'Test Time': 'test_time_s',
        'StepTime(s)': 'step_time_s',
        'Step Time': 'step_time_s',
        'Capacity(Ah)': 'maccor_capacity_ah',
        'Watt-hr': 'maccor_energy_wh',
        'Current(A)': 'current_a',
        'Voltage(V)': 'voltage_v',
        'DPt Time': 'recorded_datetime',
        'EV Temp': 'ev_temp_c',
    }
    COLUMNS_MAPPING_MACCOR_CYCLE_STATS = {
        'Cycle': 'cycle',
        'Test Time': 'test_time_s',
        'Current': 'maccor_min_current_ma',
        'Voltage': 'maccor_min_voltage_mv',
        'AH-IN': 'reported_charge_capacity_ah',
        'AH-OUT': 'reported_discharge_capacity_ah',
        'WH-IN': 'reported_charge_energy_wh',
        'WH-OUT': 'reported_discharge_energy_wh',
        'T1_Start': 'maccor_charge_thermocouple_start_c',
        'T1_End': 'maccor_charge_thermocouple_end_c',
        'T1_Min': 'maccor_charge_thermocouple_min_c',
        'T1_Max': 'maccor_charge_thermocouple_max_c',
        'T1_Start.1': 'maccor_discharge_thermocouple_start_c',
        'T1_End.1': 'maccor_discharge_thermocouple_end_c',
        'T1_Min.1': 'maccor_discharge_thermocouple_min_c',
        'T1_Max.1': 'maccor_discharge_thermocouple_max_c',
        'ACR': 'acr_ohm',
    }
 
    COLUMNS_UNSTRUCTURED_TEST_DATA = {
        'voltage_mv',
        'current_ma',
        'time_s',
        'other_details',
    }
    UNSTRUCTURED_DATA_REQUIRED_KEYS = {
        'voltage_mv',
        'current_ma',
        'pandas_read_csv_args'
    }