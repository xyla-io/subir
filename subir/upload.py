import io
import re
import pandas as pd

from . import base
from data_layer import Redshift as SQL
from .query import ColumnTypeQuery, CreateTableQuery, DropTableQuery, PrepareUploadTableQuery, AppendUploadQuery, ReplaceUploadQuery, MergeUploadQuery, MergeReplaceUploadQuery
from typing import Dict, List

class Uploader():
  def get_table_structure(self, csv_stream: io.TextIOWrapper):
    df = pd.read_csv(csv_stream, thousands=',')
    column_types = {
      base.sanitized_column_name(c): base.ColumnType.from_pd_column(df[c]).value
      for c in df
    }
    return column_types
  
  def validate_column_types(self, column_types_dictionary: Dict[str, str]):
    column_type_values = [t.value for t in base.ColumnType]
    for c, t in column_types_dictionary.items():
      if base.sanitized_column_name(c) != c:
        raise ValueError('Invalid column name', c)
      if t not in column_type_values:
        raise ValueError('Invalid column type', t)
  
  def create_table_query_text_from_stream(self, schema_name: str, table_name: str, csv_stream: io.TextIOWrapper):
    column_types = self.get_table_structure(csv_stream=csv_stream)
    return self.create_table_query(schema_name=schema_name, table_name=table_name,column_types=column_types).substituted_query

  def create_table_query(self, schema_name: str, table_name: str, column_types: Dict[str, str], read_only_groups: List[str]=[]) -> SQL.Query:
    if base.sanitized_relation_name(name=table_name) != table_name:
      raise ValueError('Invalid table name', table_name)
    self.validate_column_types(column_types_dictionary=column_types)
    return CreateTableQuery(schema=schema_name, table=table_name, column_types=column_types, read_only_groups=read_only_groups)

  def delete_table_query(self, schema_name: str, table_name: str) -> SQL.Query:
    if base.sanitized_relation_name(name=table_name) != table_name:
      raise ValueError('Invalid table name', table_name)
    return DropTableQuery(schema=schema_name, table=table_name)

  def get_column_types_result(self, schema_name: str, table_name: str) -> Dict[str, str]:
    column_type_query = ColumnTypeQuery(
      database=SQL.Layer.connection_options.database,
      schema=schema_name,
      table=table_name
    )
    return column_type_query.get_result()

  def get_column_types(self, schema_name: str, table_name: str) -> Dict[str, str]:
    if base.sanitized_relation_name(name=table_name) != table_name:
      raise ValueError('Invalid table name', table_name)
    column_results = self.get_column_types_result(schema_name=schema_name, table_name=table_name)
    return {
      c: base.ColumnType.from_query_result(t)
      for c, t in column_results.items()
    }

  def upload(self, schema_name: str, table_name: str, merge_column_names: List[str], csv_stream: io.TextIOWrapper, replace: bool=False, accept_invalid_characters: bool=False, empty_as_null: bool=False, transform_data_frame: bool=False, merge_replace: bool=False):
    column_types = self.get_column_types(schema_name=schema_name, table_name=table_name)
    type_transforms = {
      c: t.pd_type
      for c, t in column_types.items()
    }

    df = pd.read_csv(csv_stream, thousands=',')
    df.rename(base.sanitized_column_name, axis='columns', inplace=True)
    columns = list(filter(lambda s: s in type_transforms.keys(), df.columns))
    missing_columns = set(type_transforms.keys()) - set(columns)
    if missing_columns:
      raise ValueError('CSV does not contain all table columns', sorted(missing_columns))
    df = pd.DataFrame(df, columns=columns)
    self.upload_data_frame(
      schema_name=schema_name,
      table_name=table_name,
      merge_column_names=merge_column_names,
      data_frame=df,
      column_type_transform_dictionary=type_transforms,
      replace=replace,
      accept_invalid_characters=accept_invalid_characters,
      empty_as_null=empty_as_null,
      transform_data_frame=transform_data_frame,
      merge_replace=merge_replace
    )

  def upload_data_frame(self, schema_name: str, table_name: str, merge_column_names: List[str], data_frame: pd.DataFrame, column_type_transform_dictionary: Dict[str, any], replace: bool=False, accept_invalid_characters: bool=False, empty_as_null: bool=False, transform_data_frame: bool=False, merge_replace: bool=False):
    layer = SQL.Layer()

    prepare_upload_query = PrepareUploadTableQuery(
      schema=schema_name,
      table=table_name
    )
    drop_upload_query = SQL.Query(f'drop table {prepare_upload_query.upload_table};')

    layer.connect()
    layer.connection.autocommit = True
    prepare_upload_query.run(sql_layer=layer)

    try:
      layer.insert_data_frame(
        data_frame=data_frame,
        table_name=prepare_upload_query.upload_table,
        schema_name=None,
        column_type_transform_dictionary=column_type_transform_dictionary,
        accept_invalid_characters=accept_invalid_characters,
        empty_as_null=empty_as_null,
        transform_data_frame=transform_data_frame
      )

      if replace:
        combine_query = ReplaceUploadQuery(schema=schema_name, table=table_name)
      elif merge_replace:
        combine_query = MergeReplaceUploadQuery(join_columns=merge_column_names, schema=schema_name, table=table_name)
      elif merge_column_names:
        update_columns = [c for c in data_frame.columns if c not in merge_column_names]
        combine_query = MergeUploadQuery(join_columns=merge_column_names, update_columns=update_columns, schema=schema_name, table=table_name)
      else:
        combine_query = AppendUploadQuery(schema=schema_name, table=table_name)

      combine_query.run(sql_layer=layer)
    finally:
      drop_upload_query.run(sql_layer=layer)
      layer.disconnect()