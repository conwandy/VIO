import io
import numpy as np
import pandas as pd
from pandas.api import types
from django.db import models
from typing import Any, Type, Optional
from collections.abc import Mapping
from ..interfaces.sqldatabase import SQLDatabase
from ..utils.core import CoreUtilities

pd.options.mode.chained_assignment = None


class DataSet:
    """Dataset object which handles reading, altering, & exporting data via pandas"""

    _data = None

    def __init__(self, initial_data: pd.DataFrame = None):
        if initial_data is not None:
            self.data = initial_data

    def __repr__(self) -> str:
        rep = 'DataSet Object'
        return rep

    def __str__(self) -> str:
        return self.data.to_string()

    def __len__(self) -> int:
        return len(self._data)

    def from_sql_file(self, query: str, database: str, sql_format: str = None, params: Any = None) -> 'DataSet':
        """Creates dataset from a sql query"""
        self.data = SQLDatabase(database).execute_sql_file_as_dataframe(sql_text_file_location=query, sql_format=sql_format, params=params)
        return self

    def from_sql_string(self, query: str, database: str, sql_format: str = None, params: Any = None) -> 'DataSet':
        """Creates dataset from a sql string"""
        self.data = SQLDatabase(database).execute_sql_string_as_dataframe(query=query, sql_format=sql_format, params=params)
        return self

    def from_csv_text(self, csv: str) -> 'DataSet':
        """Creates dataset from csv text"""
        self.data = pd.read_csv(io.StringIO(csv))
        return self

    def from_csv_file(self, file: str, headers: list = None, encoding: str = None) -> 'DataSet':
        """creates dataset from csv file"""
        self.data = pd.read_csv(file, encoding=encoding, names=headers)
        return self

    def from_excel_file(self, file: str, default_dtype: Any = None) -> 'DataSet':
        """creates dataset from excel file"""
        self.data = pd.read_excel(file, dtype=default_dtype)
        return self

    def as_dataframe(self) -> pd.DataFrame:
        """Dataframe representation of dataset object"""
        if isinstance(self.data, pd.DataFrame):
            return self.data
        else:
            raise TypeError('Object cannot be converted to dataframe.')

    def from_dataframe(self, dataframe: pd.DataFrame) -> 'DataSet':
        """Creates dataset from a dataframe"""
        self.data = dataframe
        return self

    def from_model(self, model: Type[models.Model], filters: dict = None, columns: list = None) -> 'DataSet':
        confirmed_filters = {}
        confirmed_columns = []
        model_fields = [f.name for f in model._meta.get_fields()]
        if filters:
            confirmed_filters = {
                key: value for key, value in filters.items() if key in model_fields
            }
        if columns:
            confirmed_columns = [col for col in columns if col in model_fields]

        self.data = pd.DataFrame(list(model.objects.all().filter(**confirmed_filters).values(*confirmed_columns)))
        return self

    def set_existing_row_as_header_row(self, header_row_index: int, drop_header_row: bool = True) -> 'DataSet':
        """Sets header row and optionally drops entry"""
        if type(header_row_index) is not int:
            raise ValueError(f'header_row_index should be {int} not {type(header_row_index)}.')
        self.data.columns = self.data.iloc[header_row_index]
        if drop_header_row:
            self.data = self.data.drop(header_row_index)
        return self

    def to_csv_file(self, filepath: str, display_index: bool = False, float_format: str = '%.2f', sep: str = ',', header: bool = True) -> None:
        """Saves dataset as csv"""
        return self.data.to_csv(filepath, index=display_index, float_format=float_format, sep=sep, header=header)

    def to_excel_file(self, filepath: str, sheet_name: str = 'Sheet1', display_headers: bool = True, display_index: bool = False, float_format: str = '%.2f') -> None:
        """Saves dataset as excel"""
        self.data.to_excel(filepath, sheet_name=sheet_name, header=display_headers, index=display_index, float_format=float_format)

    def to_sql_table(self, table_name: str, connection: str, index: bool = False, if_table_exists: str = 'append') -> None:
        """Saves dataset to SQL table - Requires a SQLDatabase connection engine"""
        self.data.to_sql(table_name, connection, index=index, if_exists=if_table_exists)

    def get_sample(self, sample_size: int) -> 'DataSet':
        """Filters random dataset by sample size"""
        try:
            return DataSet(self.data.sample(int(sample_size)))
        except AttributeError as error:
            print(self.__repr__(), error)  # noqa
            return self

    def filter_column_by_value(self, column: str, value: Any) -> 'DataSet':
        """Filters a given column by a given value"""
        self.data = self.data.loc[self.data[column] == value]
        return self

    def filter_column_by_nan(self, column: str) -> 'DataSet':
        """Filters a given column by NaN"""
        self.data = self.data[self.data[column].isnull()]
        return self

    def filter_column_drop_nan(self, column: str) -> 'DataSet':
        """Filters a given column by dropping rows with values that are equal to NaN"""
        self.data = self.data[~self.data[column].isnull()]
        return self

    def drop_rows_where_all_data_is_empty(self) -> 'DataSet':
        """Drops rows where all data is NaN or None"""
        self._data = self._data.dropna(how='all')
        return self

    def add_column_fill_with_value(self, new_column: str, value: Any) -> 'DataSet':
        """Adds new column and fills it with provided value"""
        self.data[new_column] = value
        return self

    def filter_column_greater_than(self, column: str, value: Any) -> 'DataSet':
        """Filters a given column greater than a given value"""
        self.data = self.data.loc[self.data[column] > value]
        return self

    def filter_column_less_than(self, column: str, value: Any) -> 'DataSet':
        """Filters a given column less than a given value"""
        self.data = self.data.loc[self.data[column] < value]
        return self

    def filter_column_greater_than_equal_to(self, column: str, value: Any) -> 'DataSet':
        """Filters a given column greater than or equal to a given value"""
        self.data = self.data.loc[self.data[column] >= value]
        return self

    def filter_column_less_than_equal_to(self, column: str, value: Any) -> 'DataSet':
        """Filters a given column less than or equal to a given value"""
        self.data = self.data.loc[self.data[column] <= value]
        return self

    def filter_include_column_val(self, column_name: str, value: Any) -> 'DataSet':
        """Filters column by values to include"""
        self.data = self.data.loc[self.data[column_name].isin(value)]
        return self

    def filter_exclude_column_val(self, column_name: str, value: Any) -> 'DataSet':
        """Filters column by values to exclude"""
        self.data = self.data.loc[~self.data[column_name].isin(value)]
        return self

    def filter_include_columns(self, columns: list) -> 'DataSet':
        """Filters dataset by columns to include"""
        if type(columns) != list:
            raise TypeError(f'Columns should be type list not type {type(columns)}')
        self.data = self.data[columns]
        return self

    def filter_exclude_columns(self, columns: list) -> 'DataSet':
        """Filters dataset by columns to exclude"""
        if type(columns) != list:
            raise TypeError(f'Columns should be type list not type {type(columns)}')
        self.data = self.data.drop(columns, axis=1)
        return self

    def rename_columns(self, rename_dict: dict) -> 'DataSet':
        """Renames multiple columns by dict: dict = {'old_name': 'new_name', 'old_name1': 'new_name1'}"""
        self.data = self.data.rename(columns=rename_dict)
        return self

    def replace_nan_in_column(self, column: str, replacement: Any) -> 'DataSet':
        """Replace nan in given column"""
        self.data[column] = self.data[column].replace(np.nan, replacement)
        return self

    def find_and_replace_value_in_column(self, column: str, value_to_replace: Any, replacement_value: Any) -> 'DataSet':
        """Replaces a value with another in given column"""
        self.data[column] = self.data[column].replace(to_replace=value_to_replace, value=replacement_value)
        return self

    def convert_dtypes_to_numeric(self, dtype_dict: dict) -> 'DataSet':
        """Converts column type to numbers: dict = {'column1': int, 'column2': float}"""
        downcast_type_dict = {int: 'integer', float: 'float'}
        for column, dtype in dtype_dict.items():
            self.data[column] = pd.to_numeric(self.data[column], errors='coerce', downcast=downcast_type_dict[dtype])
        return self

    def convert_dtypes_to_str(self, columns: list) -> 'DataSet':
        """Converts column type to string"""
        for column in columns:
            self.data[column] = self.data[column].astype(str)
        return self

    def convert_dtypes_to_datetime(self, columns: list) -> 'DataSet':
        """Converts column type to datetime"""
        for column in columns:
            self.data[column] = pd.to_datetime(self.data[column])
        return self

    def convert_column_date_format(self, columns: list, date_format: str) -> 'DataSet':
        """Converts column datetime format"""
        for column in columns:
            self.data[column] = pd.to_datetime(self.data[column]).dt.strftime(date_format)
        return self

    def truthy_value_to_columns(self, columns: list) -> 'DataSet':
        """Applys truthy func to columns to infer bool value"""
        for column in columns:
            self.data[column] = self.data[column].apply(CoreUtilities.truthy, True)
        return self

    def drop_duplicates(self) -> 'DataSet':
        """Drops duplicate rows"""
        self.data = self.data.drop_duplicates()
        return self

    def datatypes_as_dict(self) -> Mapping:
        """Returns column dtypes as a dictionary"""
        return self.data.dtypes.to_dict()

    def columns_as_list(self) -> list:
        """Returns column names as a list"""
        return self.data.columns.tolist()

    def column_data_to_list(self, column: str, unique: bool = False) -> list:
        """Returns list of data from column"""
        return self.data[column].unique().tolist() if unique else self.data[column].tolist()

    def ceiling_column(self, column: str) -> 'DataSet':
        """Applies math ceiling opertion to column"""
        self.data[column] = self.data[column].apply(np.ceil)
        return self

    def is_column_numeric(self, column: str) -> bool:
        return types.is_numeric_dtype(self.data[column])

    def is_column_str(self, column: str) -> bool:
        return types.is_string_dtype(self.data[column])

    def is_column_bool(self, column: str) -> bool:
        return types.is_bool_dtype(self.data[column])

    def concat(self, data: pd.DataFrame | Optional['DataSet']) -> 'DataSet':
        """If data being concatenated is missing a column it will be filled with nan"""
        self.data = pd.concat([self.data, data.as_dataframe() if isinstance(data, DataSet) else data])
        return self

    def get_size(self) -> int:
        """Returns size of dataframe excluding headers"""
        if self.data is not None:
            return self.data.size

    @property
    def data(self) -> pd.DataFrame:
        return self._data

    @data.setter
    def data(self, input_data: pd.DataFrame) -> None:
        if isinstance(input_data, pd.DataFrame):
            self._data = input_data
        else:
            raise TypeError(f'DataSet set with data {type(input_data)} that is not a dataframe.')
