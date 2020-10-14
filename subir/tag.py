import os
import json
import click
import pandas as pd
import sqlalchemy as alchemy

from data_layer import Redshift as SQL
from typing import Optional, Dict, List, Tuple
from enum import Enum

class EntityType(Enum):
  ad = 'ad'
  adset = 'adset'
  campaign = 'campaign'

  @classmethod
  def from_tag_data(cls, tags: pd.DataFrame) -> any:
    return cls.ad if 'ad_id' in tags.columns else cls.adset if 'adset_tag' in tags.columns else cls.campaign

  @property
  def identifier_columns(self) -> Optional[Dict[str, any]]:
    return {
      'channel': alchemy.VARCHAR(127),
      f'{self.value}_id': alchemy.VARCHAR(127),
    }

  @property
  def columns(self) -> Optional[Dict[str, any]]:
    return {
      'company_identifier': alchemy.VARCHAR(127),
      'app': alchemy.VARCHAR(127),
      **self.identifier_columns,
      f'{self.value}_tag': alchemy.VARCHAR(255),
      f'{self.value}_subtag': alchemy.VARCHAR(255),
    }

  @property
  def id_column_names(self) -> List[str]:
    return [f'{self.value}_id']

  @property
  def tag_column_names(self) -> List[str]:
    return [f'{self.value}_tag', f'{self.value}_subtag']

  @property
  def update_column_names(self) -> List[str]:
    return self.tag_column_names + ['upload_group']

  @property
  def table_name(self) -> str:
    if self is EntityType.ad:
      return 'tag_ads'
    elif self is EntityType.adset:
      return 'tag_adsets'
    elif self is EntityType.campaign:
      return 'tag_campaigns'

  @property
  def upload_table_name(self) -> str:
    return f'upload_{self.table_name}'

  @property
  def restore_table_name(self) -> str:
    return f'restore_{self.table_name}'

def convert_id_columns(df: pd.DataFrame, col_names: List[str]):
  for name in col_names:
    df[name] = df[name].apply(lambda id: json.loads(id) if type(id) is str else None)
    df.drop(df.index[df[name].isna()], inplace=True)

def strip_empty_tags(df: pd.DataFrame, col_names: List[str], verbose: bool=False):
  if not col_names:
    return
  df[col_names] = df[col_names].fillna(value='')
  for name in col_names:
    df[name] = df[name].apply(lambda s: s.strip())
  stripped_df = df
  for name in col_names:
    stripped_df = stripped_df[stripped_df[name] == '']
  if not stripped_df.empty and verbose:
    print(f'Found {len(stripped_df)} empty tag rows')

def write_output(df: pd.DataFrame, file_name: str, description: str):
  dirname = os.path.dirname(__file__)
  path = os.path.join(dirname, 'output', file_name)
  df.to_csv(path, index=False)
  print(f'{len(df)} {description} written to {path}')

def drop_duplicates(df: pd.DataFrame, original_df: pd.DataFrame, entity: EntityType, output_prefix: str, interactive: bool=False):
  starting_rows = len(df)
  df.drop_duplicates(subset=list(entity.identifier_columns.keys()) + entity.tag_column_names, inplace=True)
  dropped_rows = starting_rows - len(df)
  if dropped_rows > 0 and interactive:
    print(f'Dropped {dropped_rows} duplicate rows with identical tags.')

  duplicated_series = df.duplicated(subset=entity.identifier_columns.keys(), keep=False)
  duplicate_rows = pd.DataFrame(df[duplicated_series.values])
  if not len(duplicate_rows):
    return True
  
  if interactive:
    duplicate_rows.ad_id = duplicate_rows.ad_id.apply(json.dumps)
    duplicate_rows['is_duplicate'] = True
    original_duplicates = original_df.sort_values(list(entity.identifier_columns.keys())).join(
      duplicate_rows[[*entity.identifier_columns.keys(), 'is_duplicate']].drop_duplicates(subset=list(entity.identifier_columns.keys())).set_index(list(entity.identifier_columns.keys())), 
      on=list(entity.identifier_columns.keys())
    )
    duplicates = original_duplicates[original_duplicates.is_duplicate == True]
    non_duplicates = original_duplicates[original_duplicates.is_duplicate != True]
    write_output(
      df=duplicates.drop('is_duplicate', axis=1),
      file_name=f'{output_prefix}_conflicting_tags.csv',
      description='conflicting tag rows'
    )
    write_output(
      df=non_duplicates.drop('is_duplicate', axis=1),
      file_name=f'{output_prefix}_non_conflicting_tags.csv',
      description='non conflicting tag rows'
    )
    resolution = click.prompt('Resolve conflicting tag rows by taking (f)irst, (l)ast, (s)kip or (a)bort', type=click.Choice(['f', 'l', 's', 'a']))
    if resolution == 'a':
      return False
  else:
    resolution = 'l'

  keep_map = {
    'f': 'first',
    'l': 'last',
    's': False,
  }
  df.drop_duplicates(subset=list(entity.identifier_columns.keys()), keep=keep_map[resolution], inplace=True)
  return True

def count_tags(schema: str, entity: EntityType):
  layer = SQL.Layer()
  layer.connect()
  count_query = SQL.Query(f'select count(*) from {schema}.{entity.table_name};')
  result = count_query.run(sql_layer=layer).fetchone()
  layer.disconnect()
  return result[0]

def upload_tags(schema: str, entity: EntityType, tags: pd.DataFrame, replace: bool=False, purge: bool=False):
  print(f'Uploading {len(tags)} tags to schema {schema}')
  layer = SQL.Layer()

  layer.connect()
  prepare_upload_query = SQL.Query(f"""
drop table if exists {schema}.{entity.upload_table_name};
create table {schema}.{entity.upload_table_name} (like {schema}.{entity.table_name});
  """)
  prepare_upload_query.run(sql_layer=layer)
  layer.commit()
  layer.disconnect()

  layer.insert_data_frame(
    data_frame=tags,
    schema_name=schema,
    table_name=entity.upload_table_name,
    column_type_transform_dictionary=None,
  )

  layer.connect()
  count_query = SQL.Query(f'select count(*) from {schema}.{entity.table_name};')
  count = count_query.run(sql_layer=layer).fetchone()[0]

  if count:
    backup_query = SQL.Query(f"""
drop table if exists {schema}.{entity.restore_table_name};
create table {schema}.{entity.restore_table_name} (like {schema}.{entity.table_name});
insert into {schema}.{entity.restore_table_name} select * from {schema}.{entity.table_name};
    """)
    backup_query.run(sql_layer=layer)

    if replace:
      truncate_query = SQL.Query(f'truncate table {schema}.{entity.table_name}')
      truncate_query.run(sql_layer=layer)

  merge_query = SQL.MergeQuery(
    join_columns=list(entity.identifier_columns.keys()),
    update_columns=entity.update_column_names,
    source_table = entity.upload_table_name,
    target_table = entity.table_name,
    source_schema = schema,
    target_schema = schema
  )
  merge_query.run(sql_layer=layer)

  if purge:
    condition_queries = [
      SQL.Query(f'("{c}" = %s or "{c}" is null)', substitution_parameters=('',)) 
      for c in entity.tag_column_names
    ]
    conditions_query_text = 'and '.join(q.query for q in condition_queries)
    purge_query = SQL.Query(
      query=f'''
delete from {schema}.{entity.table_name}
where {conditions_query_text};
      ''',
      substitution_parameters=tuple(p for q in condition_queries for p in q.substitution_parameters)
    )
    purge_query.run(sql_layer=layer)

    convert_empty_query = SQL.Query(f'''
update {schema}.{entity.table_name}
set {entity.value}_subtag = null
where {entity.value}_subtag = '';
    ''')
    convert_empty_query.run(sql_layer=layer)

  drop_upload_query = SQL.Query(f'drop table if exists {schema}.{entity.upload_table_name};')
  drop_upload_query.run(sql_layer=layer)
  layer.commit()

class Tagger:
  def apply_tags(self, schema_name: str, entity_name: str, should_drop: bool, should_purge: bool, csv_stream: any, file_name: str, interactive: bool=False):
    original_df = pd.read_csv(csv_stream, dtype='object')
    original_df.rename(columns={'Unnamed: 0': ''}, inplace=True)
    if interactive:
      print(f'Imported {len(original_df)} tag rows from stream {csv_stream}')
    df = original_df.copy()
    df.rename(columns={'company': 'company_identifier'}, inplace=True)

    entity = EntityType.from_tag_data(tags=df) if entity_name == 'auto' else EntityType(entity_name)
    df = pd.DataFrame(df, columns=list(entity.columns.keys()))
    convert_id_columns(df, entity.id_column_names)
    strip_empty_tags(df, entity.tag_column_names, verbose=interactive)
    if not drop_duplicates(
      df=df, 
      original_df=original_df, 
      entity=entity,
      output_prefix=os.path.splitext(os.path.basename(file_name))[0],
      interactive=interactive
    ):
      return 0

    if df.empty:
      if interactive:
        print('No tags found in data')
      return 0

    df['upload_group'] = os.path.basename(file_name)
      
    if interactive:
      print(df.head())
      count = count_tags(schema=schema_name, entity=entity)
    if interactive:
      verb = 'Replace' if should_drop else 'Merge'
      confirmation = click.prompt(f'{verb} {count} existing {entity.value} tags with {len(df)} new tags for {schema_name}', type=click.Choice(['y', 'n']))
      if confirmation.lower() != 'y':
        return 0
    
    upload_tags(schema=schema_name, entity=entity, tags=df, replace=should_drop, purge=should_purge)
    if interactive:
      final_count = count_tags(schema=schema_name, entity=entity)
      print(f'{final_count} {entity.value} tags for {schema_name} exist after upload')

    return len(df)