import argparse
from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()
parser.add_argument('--table_name', type=str, required=True, help='Nome da tabela a ser processada')
args = parser.parse_args()

spark = (
    SparkSession.builder
    .appName(f'transform_bronze_{args.table_name}')
    .getOrCreate()
)

df = spark.read.parquet(f's3a://landing/{args.table_name}/')

df.write.mode('overwrite').parquet(f's3a://bronze/{args.table_name}/')

spark.stop()