#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2018 Ryan Mackenzie White <ryan.white4@canada.ca>
#
# Distributed under terms of the  license.

"""
Example code for reading/writing different file formats
Includes reading / writing via byte buffers to pfs
Also available in a notebook
"""

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.feather as feather
import csv
from io import StringIO
import timeit

df = pd.DataFrame({'one':[-1, np.nan, 2.5],
                    'two':['foo','bar','baz'],
                    'three':[True, False, True]})
start_time = timeit.default_timer()
df.to_csv('example.csv')
print(timeit.default_timer() - start_time)

table = pa.Table.from_pandas(df)

start_time = timeit.default_timer()
pq.write_table(table, 'example.parquet')
print(timeit.default_timer() - start_time)

start_time = timeit.default_timer()
table2 = pq.read_table('example.parquet')
print(timeit.default_timer() - start_time)

table2.to_pandas()

pq.read_table('example.parquet', columns=['one', 'three'])

parquet_file = pq.ParquetFile('example.parquet')

parquet_file.metadata

parquet_file.schema

parquet_file.read_row_group(0)

writer = pq.ParquetWriter('example2.parquet', table.schema)
for i in range(3):
    writer.write_table(table)

writer.close() 
pf2 = pq.ParquetFile('example2.parquet')
pf2.num_row_groups

with pq.ParquetWriter('example3.parquet', table.schema) as writer:
    for i in range(3):
        writer.write_table(table)

context = pa.default_serialization_context()

start_time = timeit.default_timer()
serialized_df = context.serialize(df)
print(timeit.default_timer() - start_time)

df_components = serialized_df.to_components()

start_time = timeit.default_timer()
original_df = context.deserialize_components(df_components)
print(timeit.default_timer() - start_time)

original_df
data = {
        i: np.random.randn(500, 500)
        for i in range(100)
        }
buf = pa.serialize(data).to_buffer()
type(buf)
buf.size
restored_data = pa.deserialize(buf)
restored_data[0]
feather.write_feather(df, 'example.feather')
read_df = feather.read_feather('example.feather')
with open('example2.feather', 'wb') as f:
    feather.write_feather(df, f)

with open('example2.feather', 'rb') as f:
    read_df = feather.read_feather(f)

# StringIO buffer
buffer = StringIO()
df.to_csv(buffer)
print(buffer)
buffer += 'Add a line, 2, buffer'
print(buffer)
