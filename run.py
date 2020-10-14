import os
import io
import re
import click

from config import sql_config
from data_layer import Redshift as SQL
from subir import Uploader, EntityType, Tagger
from typing import Tuple

all_database_values = sql_config.keys()

class Subir:
  database_name: str
  
  def __init__(self, database_name: str):
    self.database_name = database_name

  def path_to_table_name(self, path: str) -> str:
    return re.sub(r'[^a-zA-Z0-9]', '_', os.path.splitext(os.path.basename(path))[0]).lower()

@click.group()
@click.option('-db', '--database', 'database_name', type=click.Choice(all_database_values), default='stage_01')
@click.pass_context
def run(ctx: any, database_name: str):
  ctx.obj = Subir(database_name=database_name)
  SQL.Layer.configure_connection(sql_config[ctx.obj.database_name])

@run.command()
@click.option('-s', '--schema', 'schema_name', type=str, required=True)
@click.option('-t', '--table', 'table_name', type=str)
@click.argument('csv_file', type=click.File('r'))
@click.pass_obj
def create(subir: Subir, schema_name: str, table_name: str, csv_file: io.TextIOWrapper):
  table = table_name if table_name else subir.path_to_table_name(csv_file.name)
  uploader = Uploader()
  query_text = uploader.create_table_query_text_from_stream(schema_name=schema_name, table_name=table, csv_stream=csv_file)
  output_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'output', f'create_{table}.sql')
  edit = True
  while edit:
    with open(output_path, 'w') as query_file:
      query_file.write(query_text)
    print(f'Create table query for {schema_name}.{table}\n\n{query_text}\n\n')
    if click.confirm(f'The query has been written to\n{output_path}\nWould you like to modify it?'):
      os.system('%s %s' % ('vi', output_path))
      with open(output_path, 'r') as query_file: 
        query_text = query_file.read()
    else:
      edit = False
  query = SQL.Query(query_text.replace('%', '%%'))
  layer = SQL.Layer()
  layer.connect()
  query.run(sql_layer=layer)
  layer.commit()
  layer.disconnect()

@run.command()
@click.option('-s', '--schema', 'schema_name', type=str, required=True)
@click.option('-t', '--table', 'table_name', type=str)
@click.option('-m', '--merge', 'merge_column_names', type=str, multiple=True)
@click.option('-d', '--drop', 'drop_existing', is_flag=True)
@click.argument('csv_file', type=click.File('r'))
@click.pass_obj
def upload(subir: Subir, schema_name: str, table_name: str, merge_column_names: Tuple[str], drop_existing: bool, csv_file: io.TextIOWrapper):
  table = table_name if table_name else subir.path_to_table_name(csv_file.name)
  uploader = Uploader()
  uploader.upload(
    schema_name=schema_name,
    table_name=table,
    merge_column_names=[c.lower() for c in merge_column_names],
    replace=drop_existing,
    csv_stream=csv_file
  )

@run.command()
@click.option('-s', '--schema', 'schema_name', type=str, required=True)
@click.option('-e', '--entity', 'entity_name', type=click.Choice([e.value for e in EntityType] + ['auto']), default='auto')
@click.option('-d', '--drop-existing', 'should_drop', is_flag=True)
@click.option('--purge-empty/--no-purge-empty', 'should_purge', default=True)
@click.argument('csv_file', type=click.File('r'))
@click.pass_obj
def tag(subir: Subir, schema_name: str, entity_name: str, should_drop: bool, should_purge: bool, csv_file: io.TextIOWrapper):
  tagger = Tagger()
  tagger.apply_tags(
    schema_name=schema_name,
    entity_name=entity_name,
    should_drop=should_drop,
    should_purge=should_purge,
    csv_stream=csv_file,
    file_name=csv_file.name,
    interactive=True
  )

if __name__ == '__main__':
  run()