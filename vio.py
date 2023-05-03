import pandas as pd
import datetime
import os


class Vio:
    speed_columns = {
        '4 SPEEDS': 'AUTO_4_SPEED_PCT',
        '5 SPEEDS': 'AUTO_5_SPEED_PCT',
        '6 SPEEDS': 'AUTO_6_SPEED_PCT',
        '6_7 SPEEDS': 'AUTO_6_7_SPEED_PCT',
        '7_8 SPEEDS': 'AUTO_7_8_SPEED_PCT',
        '8 SPEEDS': 'AUTO_8_SPEED_PCT',
        '10 SPEEDS': 'AUTO_10_SPEED_PCT',
        'CVTs': 'CVT_PCT'
    }

    columns_to_fill = ['TOTAL', 'AUTO_4_SPEED_PCT', 'AUTO_5_SPEED_PCT', 'AUTO_6_SPEED_PCT',
                       'AUTO_6_7_SPEED_PCT', 'AUTO_7_8_SPEED_PCT', 'AUTO_8_SPEED_PCT', 'AUTO_10_SPEED_PCT',
                       'CVT_PCT']

    def __init__(self, vio_file: str):
        self.input_data = pd.read_csv(vio_file)
        self.data = self._format_data(self.input_data)

        for i, h in self.speed_columns.items():
            self.data[i] = 0


    def append_datetime_to_filename(self, filepath: str) -> str:
        basename, extension = os.path.splitext(filepath)
        now = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        return f"{basename}_{now}{extension}"

    def apply_multiply_columns(self) -> None:
        for column, percentage_columns in self.speed_columns.items():
            result = self.data['TOTAL']
            print(result)
            print('-----')
            self.data['results'] = round(result * self.data[percentage_columns], 0).astype(int)

   def apply_multiply_columns_v2(self, row) -> None:
        for column, percentage_columns in self.speed_columns.items():

            total = row['TOTAL']
            this_math = round(total * row[percentage_columns])
            print(total, this_math)
            return this_math

    def test_apply(self):
        self.data['results'] = self.data.apply(self.apply_multiply_columns_v2, axis=1)

    def _format_data(self, input_data: pd.DataFrame) -> pd.DataFrame:
        this_data = input_data.copy()
        this_data = this_data.dropna(subset=['BASE VEHICLE ID', 'LITERS', 'DRIVE WHEELS']).reset_index(drop=True)

        this_data['DRIVE WHEELS'].replace({'4RD': 'AWD', '4FD': 'AWD'}, inplace=True)

        bv_id = 'BASE VEHICLE ID'
        liters = 'LITERS'
        drive_type = 'DRIVE WHEELS'
        this_data['Key'] = f"{this_data[bv_id].astype(str).str.rstrip('0')}{this_data[liters].astype(str)}.{this_data[drive_type]}"

        this_data[self.columns_to_fill] = this_data[self.columns_to_fill].fillna(0)
        return this_data

    def get_speed_breakout_row(row, num_speeds, speed_pct_header, vio_year):
        this_new_row = row.copy()
        if speed_pct_header == 'CVT_PCT':
            this_new_row['Key'] = this_new_row['Key']
        else:
            this_new_row['Key'] = this_new_row['Key'] + '.' + str(num_speeds)
        this_new_row['TOTAL'] = round(this_new_row['TOTAL'] * this_new_row[speed_pct_header])
        this_new_row['Speeds'] = num_speeds
        this_new_row['VIO Year'] = vio_year
        return this_new_row

    speed_breakout_route = {
        '4 SPEEDS': [4, 'AUTO_4_SPEED_PCT'],
        '5 SPEEDS': [5, 'AUTO_5_SPEED_PCT'],
        '6 SPEEDS': [6, 'AUTO_6_SPEED_PCT'],
        '6_7 SPEEDS': [6, 'AUTO_6_7_SPEED_PCT'],
        '8 SPEEDS': [8, 'AUTO_8_SPEED_PCT'],
        '7_8 SPEEDS': [8, 'AUTO_7_8_SPEED_PCT'],
        '10 SPEEDS': [10, 'AUTO_10_SPEED_PCT'],
        'CVTs': ['CVT', 'CVT_PCT']
    }
