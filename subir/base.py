from __future__ import annotations

import re
import pandas as pd

from enum import Enum
from typing import Dict

class ColumnType(Enum):
  integer = 'bigint'
  decimal = 'double precision'
  date = 'date'
  short_text = 'character varying(256)'
  medium_text = 'character varying(8192)'
  long_text = 'character varying(32768)'
  boolean = 'boolean'

  @classmethod
  def from_pd_column(cls, pd_column: any) -> ColumnType:
    type_name = str(pd_column.dtype)
    if type_name == 'int64':
      return cls.integer
    elif type_name == 'float64':
      return cls.decimal
    elif type_name == 'bool':
      return cls.boolean
    elif type_name == 'object':
      length = pd_column.apply(str).apply(len).max()
      if length > 2048:
        return cls.long_text
      if length < 64:
        if cls._pd_column_is_date(pd_column):
          return cls.date
        return cls.short_text
      return cls.medium_text
    else:
      return cls.long_text
  
  @classmethod
  def from_query_result(cls, result: str) -> ColumnType:
    try:
      return cls(result)
    except ValueError as e:
      if result.find('character varying') == 0 or result.find('USER-DEFINED') == 0:
        return cls.long_text
      elif result.find('integer') == 0:
        return cls.integer
      elif result.find('numeric') == 0:
        return cls.decimal
      elif result.find('timestamp') == 0:
        return cls.date
      raise e

  @classmethod
  def _pd_column_is_date(self, pd_column: any) -> bool:
    empty = True
    for value in pd_column:
      if not value or pd.isna(value):
        continue
      empty = False
      if not re.match(r'[0-9]{1,2}[/-][0-9]{1,2}[/-][0-9]{2,4}', value) and not re.match(r'[0-9]{2,4}[/-][0-9]{1,2}[/-][0-9]{1,2}', value):
        return False
    return not empty

  @property
  def pd_type(self) -> any:
    if self is ColumnType.integer:
      return pd.Int64Dtype()
    elif self is ColumnType.decimal:
      return float
    elif self is ColumnType.boolean:
      return bool
    elif self is ColumnType.date:
      return 'datetime64[ns]'
    else:
      return 'object'

def sanitized_relation_name(name: str) -> str:
  return re.sub(r'[^a-z0-9_]', '_', name.lower())

def sanitized_column_name(name: str) -> str:
  return re.sub(r'[^a-z0-9_ ]', '_', name.lower())