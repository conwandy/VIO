import pandas as pd
import os
from datetime import datetime


def append_datetime_to_filename(filepath):
    """
    Appends the current date and time to the end of a file name before the extension.
    """
    basename, extension = os.path.splitext(filepath)
    now = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    return f"{basename}_{now}{extension}"

# Define the columns to keep


columns_to_keep = ['YEAR MODEL', 'BASE VEHICLE ID', 'DRIVE WHEELS', 'LITERS', 'TOTAL', 'AUTO_4_SPEED_PCT',
                   'AUTO_5_SPEED_PCT', 'AUTO_6_SPEED_PCT', 'AUTO_6_7_SPEED_PCT',
                   'AUTO_7_8_SPEED_PCT', 'AUTO_8_SPEED_PCT', 'AUTO_10_SPEED_PCT', 'CVT_PCT']

# Read the CSV file and keep only the selected columns
filename = '/Users/aconway/Desktop/ETE_US_202210.csv'
df = pd.read_csv(filename, usecols=columns_to_keep)
file_year = filename[-10:-6]
df['VIO Year'] = file_year

# Remove rows where 'BASE VEHICLE ID', 'LITERS' or 'DRIVE WHEELS' are blank
df = df.dropna(subset=['BASE VEHICLE ID', 'LITERS', 'DRIVE WHEELS']).reset_index(drop=True)

# Replace '4RD' and '4FD' with 'AWD' in the 'DRIVE WHEELS' column
df['DRIVE WHEELS'].replace({'4RD': 'AWD', '4FD': 'AWD'}, inplace=True)

# Add a new column called 'Key' that concatenates 'BASE VEHICLE ID' with 'LITERS' and 'DRIVE WHEELS'
df['Key'] = df['BASE VEHICLE ID'].astype(str).str.rstrip('0') + df['LITERS'].astype(str) + '.' +\
                       df['DRIVE WHEELS']

# Fill in missing values with 0 in the selected columns
columns_to_fill = ['TOTAL', 'AUTO_4_SPEED_PCT', 'AUTO_5_SPEED_PCT', 'AUTO_6_SPEED_PCT',
                   'AUTO_6_7_SPEED_PCT', 'AUTO_7_8_SPEED_PCT', 'AUTO_8_SPEED_PCT', 'AUTO_10_SPEED_PCT', 'CVT_PCT']

df[columns_to_fill] = df[columns_to_fill].fillna(0)


# Multiply the following columns
speed_columns = {
    '4 SPEEDS': ['AUTO_4_SPEED_PCT'],
    '5 SPEEDS': ['AUTO_5_SPEED_PCT'],
    '6 SPEEDS': ['AUTO_6_SPEED_PCT'],
    '6 7 SPEEDS': ['AUTO_6_7_SPEED_PCT'],
    '7 8 SPEEDS': ['AUTO_7_8_SPEED_PCT'],
    '8 SPEEDS': ['AUTO_8_SPEED_PCT'],
    '10 SPEEDS': ['AUTO_10_SPEED_PCT'],
    'CVTs': ['CVT_PCT']
}


def multiply_columns(df, column_list):
    result = df['TOTAL']
    for speed_column in column_list[:-1]:
        result *= df[speed_column]
    return round(result * df[column_list[-1]], 0).astype(int)


for column, percentage_columns in speed_columns.items():
    df[column] = multiply_columns(df, percentage_columns)

# Create a new DataFrame to store the duplicated rows


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
    '6 7 SPEEDS': [6, 'AUTO_6_7_SPEED_PCT'],
    '8 SPEEDS': [8, 'AUTO_8_SPEED_PCT'],
    '7 8 SPEEDS': [8, 'AUTO_7_8_SPEED_PCT'],
    '10 SPEEDS': [10, 'AUTO_10_SPEED_PCT'],
    'CVTs': ['CVT', 'CVT_PCT']
}


def get_speed_breakout_df(df, speed_route, vio_year):
    duplicated_rows = pd.DataFrame(columns=df.columns)
    for row in df.itertuples():
        for key, value in speed_route.items():
            if getattr(row, key) > 0:
                num_speeds = value[0]
                speed_pct_header = value[1]
                new_row = get_speed_breakout_row(row, num_speeds, speed_pct_header, vio_year)
                duplicated_rows = duplicated_rows.append(new_row)
    return duplicated_rows

speed_breakout_df = get_speed_breakout_df(df, speed_breakout_route, file_year)


# Keep the duplicated rows Dataframe
df = speed_breakout_df

# Rename the 'TOTAL' column to 'VIO'
df = df.rename(columns={'TOTAL': 'VIO'})

# Group rows
df = df.groupby(['Key', 'YEAR MODEL', 'Speeds', 'VIO Year'])['VIO'].sum().reset_index()

# Reorder columns
df = df[['Key', 'YEAR MODEL', 'Speeds', 'VIO', 'VIO Year']]

# Sort the DataFrame by the 'vehicle id' column
df.sort_values(by='Key', inplace=True)

dated_file_path = append_datetime_to_filename('vio_result.csv')

# Export the final dataframe as a CSV file
df.to_csv(dated_file_path, index=False)