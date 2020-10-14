from data_layer import Redshift as SQL
from typing import Optional, Dict, List

class ColumnTypeQuery(SQL.GeneratedQuery, SQL.ResultQuery[Dict[str, str]]):
  database: str
  schema: str
  table: str

  def __init__(self, database: str, schema: str, table: str):
    self.database = database
    self.schema = schema
    self.table = table
    super().__init__()

  def generate_query(self):
    self.query = '''
select column_name, data_type, character_maximum_length
from information_schema.columns
where table_catalog = %s
and table_schema = %s
and table_name = %s;
    '''
    self.substitution_parameters=(
      self.database,
      self.schema,
      self.table,
    )

  def cursor_to_result(self, cursor: any) -> Optional[Dict[str, str]]:
    result = cursor.fetchall()
    return {
      r[0]: f'{r[1]}({r[2]})' if r[1] == 'character varying' else r[1]
      for r in result
    }

class CreateTableQuery(SQL.GeneratedQuery):
  schema: str
  table: str
  column_types: Dict[str, str]
  read_only_groups: List[str]

  def __init__(self, schema: str, table: str, column_types: Dict[str, str], read_only_groups: List[str]=[]):
    self.schema = schema
    self.table = table
    self.column_types = column_types
    self.read_only_groups = read_only_groups
    super().__init__()

  def generate_query(self):
    columns_definition = ',\n'.join(f'  "{c}" {t}' for c, t in self.column_types.items())
    self.query = f'''
create table {self.schema}.{self.table} (
{columns_definition}
);
    '''
    for group in self.read_only_groups:
      self.query += f'\ngrant select on table {self.schema}.{self.table} to group {group};'

class DropTableQuery(SQL.GeneratedQuery):
  schema: str
  table: str

  def __init__(self, schema: str, table: str):
    self.schema = schema
    self.table = table
    super().__init__()

  def generate_query(self):
    self.query = f'drop table {self.schema}.{self.table}'

class UploadQuery(SQL.GeneratedQuery):
  schema: str
  table: str

  def __init__(self, schema: str, table: str):
    self.schema = schema
    self.table = table
    super().__init__()

  @property
  def upload_table(self) -> str:
    return f'flx_upload_{self.table}'

class PrepareUploadTableQuery(UploadQuery):
  schema: str
  table: str

  def generate_query(self):
    self.query = f'''
create temporary table {self.upload_table} (like {self.schema}.{self.table} including defaults);
    '''

class AppendUploadQuery(UploadQuery):
  def generate_query(self):
    self.query = f'insert into {self.schema}.{self.table} select * from {self.upload_table};'

class ReplaceUploadQuery(UploadQuery):
  def generate_query(self):
    append_query = AppendUploadQuery(schema=self.schema, table=self.table)
    self.query = f'''
truncate table {self.schema}.{self.table};
{append_query.query}
    '''
    self.substitution_parameters = append_query.substitution_parameters

class MergeUploadQuery(SQL.MergeQuery):
  schema: str
  table: str

  def __init__(self, join_columns: List[str], update_columns: List[str], schema: str, table: str):
    self.schema = schema
    self.table = table

    super().__init__(
      join_columns=join_columns,
      update_columns=update_columns,
      source_table=self.upload_table,
      target_table=table,
      source_schema=None,
      target_schema=schema
    )

  @property
  def upload_table(self) -> str:
    return f'flx_upload_{self.table}'

class MergeReplaceUploadQuery(SQL.MergeReplaceQuery):
  schema: str
  table: str

  def __init__(self, join_columns: List[str], schema: str, table: str):
    self.schema = schema
    self.table = table

    super().__init__(
      join_columns=join_columns,
      source_table=self.upload_table,
      target_table=table,
      source_schema=None,
      target_schema=schema
    )

  @property
  def upload_table(self) -> str:
    return f'flx_upload_{self.table}'