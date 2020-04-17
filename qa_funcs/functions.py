from pyspark.sql.functions import approx_count_distinct, col, datediff, to_date, count, when, unix_timestamp, format_number, lit
from pyspark.sql.types import TimestampType
from pyspark.sql import SparkSession, Row
import pandas as pd

spark = SparkSession.builder.appName("dgi-spark").getOrCreate()

def fusion_intersection(df, key, df_truth):
    """Primary key quantification based on source of truth for a particular dataset, e.g. does the population of device_id in a new feed match our datasource of record as anticipated

    Arguments:
    df {pyspark.dataframe} -- [dataframe to analyze]
    key [string] -- [column name of primary key in dataframe]
    df_truth {pyspark.dataframe} -- [selected dataframe column with primary key source of truth]
    Returns:
    [dict] -- dictionary of results quantifying 1) intersection of primary key values, 2) percentage of primary key values in df that intersect with df_truth, 3) primary key values in df that are not in df_truth, 4) primary key values that are in only df or df_truth and do not intersect
    [list] -- intersection of primary key values between df and df_truth
    [list] -- primary key values in df that are not in df_truth
    [list] -- primary key values that are in only df or df_truth and do not intersect
    """

    source_df_list = set(df_truth.select(key).distinct().collect())
    target_df_list = set(df.select(key).distinct().collect())

    intersection = [i[0] for i in list(source_df_list.intersection(target_df_list)) if i[0] is not None]
    difference = [i[0] for i in list(target_df_list.difference(source_df_list)) if i[0] is not None]
    symmetric_difference = [i[0] for i in list(source_df_list.symmetric_difference(target_df_list)) if i[0] is not None]

    output_count = {'intersection':len(intersection), 'intersection_percent': len(intersection) / len(source_df_list), 'difference':len(difference), 'symmetric_difference':len(symmetric_difference)}

    return output_count, sorted(intersection), sorted(difference), sorted(symmetric_difference)

def completeness(df, target_cols):
    """Completeness quantification based on truth for a particular dataset and key
    e.g. does the population of device_id in a new feed suffice non-null record threshold as anticipated

    Arguments:
    df {pyspark.dataframe} -- [dataframe to analyze]
    target_cols [list] -- [columns within dataframe to examine for completeness]
    Returns:
    [dict] -- dictionary of results showing percent null values in each target column
    """

    pct_complete = df.agg(*((100 - format_number((count(when(col(c).isNull(), c))/ df.count() * 100),3)).alias(c) for c in target_cols))
    output = pct_complete.collect()[0].asDict()
    print('COMPLETENESS FUNCTION RESULTS: returning dict with percent of values in each column that are complete (not null)')
    return output

def uniqueness(df, target_cols):
    """Uniqueness - no repetitive values for data points which are unique by design; measured by proportion of duplicative values for fields identified by the business as non-duplicative
    e.g. does the population of account_id in a new feed suffice non-duplicated records as anticipated

    Arguments:
    df {pyspark.dataframe} -- [dataframe to analyze]
    target_cols [list] -- [columns within dataframe to examine for uniqueness]
    Returns:
    [dict] -- dictionary of results showing percent non-unique values in each target column
    """

    duplicates = df.agg(*((approx_count_distinct(col(c))/ df.count() * 100).alias(c) for c in target_cols))
    output = duplicates.collect()[0].asDict()

    print('UNIQUENESS FUNCTION RESULTS: returning dict with percent of values in each column that are unique')

    return output

def consistency(df,target_cols):
    """Consistency - Quantification of attribute payload variability, highlighting outliers which may signal integration problem (low count of a particular attribute only available in subset of data)
    e.g. does the population of post_pagename consist of ~90% disney:main

    Arguments:
    df {pyspark.dataframe} -- [dataframe to analyzed]
    target_cols [list] -- [columns within dataframe to examine for consistency]
    Returns:
    [dict] -- dictionary of results showing largest percent of each target column composed by a certain value
    """
    output = Row(**{c:(df.groupBy(c).count().orderBy('count', ascending=False).select('count').first()[0] / df.select(c).count() * 100) for c in target_cols}).asDict()

    print('CONSISTENCY FUNCTION RESULTS: returning dict with largest percent of data in each column composed by a certain value')

    return output

def precision(df, schema_map):
    """Precision - degree to which data correctly describes an event it represents; measured by proportion of values that represent accurate events based on expected data types

    Arguments:
    df {pyspark.dataframe} -- [dataframe with schema to be analyzed]
    schema_map [dict] -- [dictionary of column_name:datatype pairs showing expected schema with PySpark DataTypes (e.g. 'partner':'StringType')]
    Returns:
    [dict] -- dictionary of results showing field names, expected type, and actual type where there are inconsistencies
    """

    df_schema = dict(zip(df.schema.names, [str(f.dataType) for f in df.schema.fields]))
    output = {}
    accepted_types = ['ByteType', 'ShortType', 'IntegerType','LongType', 'DoubleType','FloatType', 'DecimalType', 'BinaryType','BooleanType','StringType','TimestampType','DateType','DecimalType(38,0)']

    for c in list(df_schema.keys()):
        if df_schema[c] not in accepted_types:
            output[c] = 'Unacceptable data type: {}'.format(df_schema[c])
        elif df_schema[c] == schema_map[c]:
            output[c] = 'Correct data type'
        else:
            output[c] = 'Incorrect data type: Expected = {}, Actual = {}'.format(schema_map[c], df_schema[c])

    print('PRECISION FUNCTION RESULTS: returning result of schema validation')

    return output

def latency(restricted_event_df, time_of_action, target_timestamp):
    """Latency - degree to which data represents reality from the required point in time; measured by time difference in days
    e.g. do timestamps of clickpath actions match expected timestamps from a known variable for a uniquely identified event

    Arguments:
    restricted_event_df {pyspark.dataframe} -- [dataframe with action timestamps to be analyzed, there should only be one event and row passed into function]
    time_of_action [timestamp] -- [time which actual action was triggered]
    target_timestamp [string] -- [timestamp column in dataset]
    Returns:
    [dict] -- dictionary of results including latency_days which is the difference between the timestamp and actual action in days
    """

    if restricted_event_df.count() == 1:
        df = restricted_event_df.withColumn("latency_days", datediff(to_date(lit(time_of_action)), (target_timestamp)))
        df = df.select('latency_days', target_timestamp).toPandas().to_dict(orient='list')
        return df
    else:
        raise Exception('pass dataframe with a single event only')

def stability(df, ingestion_time_col):
    """Stability - Quantification of successful data ingestions and availability over a period of observation time to confirm stability of data feed
    e.g. What are the last 3-5 ingestion timestamps and how do they differ

    Arguments:
    df {pyspark.dataframe} -- [dataframe with ingestion timestamps to be analyzed]
    ingestion_time_col [string] -- [column name in df with ingestion times]
    Returns:
    [dict] -- dictionary of results showing mean and median time difference (measured in seconds) between ingestion timestamps
    """

    if isinstance(df.schema[ingestion_time_col].dataType, TimestampType):
        df = df.withColumn(ingestion_time_col, unix_timestamp(ingestion_time_col))
    ingestion_times = df.select(ingestion_time_col).distinct().sort(ingestion_time_col, ascending = False).rdd.flatMap(lambda x: x).collect()

    output = [(ingestion_times[i] - ingestion_times[i+1])/3600 for i in range(len(ingestion_times) - 1)]

    print('STABILITY FUNCTION RESULTS: returning time differefence between ingestions (measured in hours)')

    return output

def accuracy(df):
    """Accuracy - Analysis that data attributes correspond to reality and intended usage, validation of actual values in dataset by type

    Arguments:
    df {pyspark.dataframe} -- [dataframe with values to be analyzed]
    Returns:
    [dict] -- dictionary of results showing field names, along with results for validation of correct data type and format over all rows
    """

    df = df.sample(False, 0.2, 42)
    accepted_types = ['BinaryType','BooleanType','DoubleType','FloatType','IntegerType','LongType','StringType','TimestampType','DateType','DecimalType','DecimalType(38,0)']
    df_schema = dict(zip(df.schema.names, [str(f.dataType) for f in df.schema.fields]))
    pattern_dict = {'StringType':".*",
    'LongType': "^[0-9]*$",
    'IntegerType': "^[0-9]*$",
    'DecimalType': "^[0-9]*$",
    'DecimalType(38,0)': "^[0-9]*$",
    'DoubleType': "[+-]?([0-9]*[.])?[0-9]+",
    'FloatType': "[+-]?([0-9]*[.])?[0-9]+",
    'TimestampType': "[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[1-2][0-9]|3[0-1]) (2[0-3]|[01][0-9]):[0-5][0-9]:[0-5][0-9]",
    'DateType': "[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[1-2][0-9]|3[0-1])",
    'BinaryType': '^[0-1]$',
    'BooleanType': '^(?i)(true|false)$'}
    output = {}

    for c in list(df_schema.keys()):

      if df_schema[c] not in accepted_types:
        output[c] = 'Unacceptable data type: {}'.format(df_schema[c])
      else:
        matched_rows = df.select(c).filter(df[c].rlike(pattern_dict[df_schema[c]])).count()
        total_rows = df.count()
        if matched_rows == total_rows:
          output[c] = 'Data format validated for all rows'
        else:
          output[c] = 'Data format invalid for {} of {} rows'.format(total_rows - matched_rows, total_rows)

    print('ACCURACY FUNCTION RESULTS: returning result of data format validation')

    return output

def main(dataset):
  """Main function to return dataframe of results for DQ Certification -- excludes Fusion Intersection, Stability, and Latency

  Arguments:
  dataset {pyspark.dataframe} -- [Dataset to be analyzed, input should be the return from function load_snowflake_table e.g. load_snowflake_table(dbutils, "dss_prod.dss.ctam_character")]
  Returns:
  {Pandas dataframe} -- dataframe with results from DQ Certification functions"""

  schema_map = dict(zip(dataset.schema.names, [str(f.dataType) for f in dataset.schema.fields]))
  completeness_results = completeness(dataset, [i for i in dataset.columns])
  uniqueness_results = uniqueness(dataset, [i for i in dataset.columns])
  consistency_results = consistency(dataset, [i for i in dataset.columns])
  accuracy_results = accuracy(dataset)
  precision_results = precision(dataset, schema_map)

  completeness_results = {k:round(v,2) for k,v in completeness_results.items()}
  uniqueness_results = {k:round(v,2) if v < 100 else 100 for k,v in uniqueness_results.items()}
  consistency_results = {k:round(v,2) for k,v in consistency_results.items()}

  all_results = pd.DataFrame.from_dict([completeness_results, uniqueness_results, consistency_results, accuracy_results, precision_results]).transpose()
  all_results.columns = ['completeness','uniqueness','consistency','accuracy','precision']

  return all_results