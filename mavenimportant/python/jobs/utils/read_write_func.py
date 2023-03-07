"""
spark读写模块，
添加读取数据库解密
"""
import pandas as pd
from pyspark.sql import DataFrame, SparkSession


class SparkReaderWriter(object):
    """spark读写模块，个性化方法"""

    def __init__(self, spark_or_df, mode):
        """
        初始化
        :param spark_or_df:
        :param mode:
        """
        self._spark_or_df = spark_or_df
        self._mode = mode
        if isinstance(spark_or_df, DataFrame):
            self.spark_read_or_write = self._spark_or_df.write.mode(self._mode)
            self._read_or_write = "write"
        elif isinstance(spark_or_df, SparkSession):
            self.spark_read_or_write = self._spark_or_df.read
            self._read_or_write = "read"
        else:
            raise TypeError("spark_or_df parameter input error, must be a DataFrame or SparkSession of types")

    def _load_or_save(self):
        """
        判断读写得
        :return:
        """
        if self._read_or_write == "read":
            return self.spark_read_or_write.load()
        elif self._read_or_write == "write":
            return self.spark_read_or_write.save()
        else:
            raise TypeError(f"read_or_write type :{type(self._read_or_write)}; value :{self._read_or_write}")

    def relational_db(self, url, driver, user, password, query_sql_or_table, **options):
        """
        读取、写入关系型数据库方法
        :param url:
        :param driver:
        :param user:
        :param password:
        :param query_sql_or_table:
        :param options:
        :return:
        """
        self.spark_read_or_write.format("jdbc") \
            .option("url", url) \
            .option("driver", driver) \
            .option("user", user) \
            .option("password", password)

        if self._read_or_write == "read":
            self.spark_read_or_write.option("dbtable", f"({query_sql_or_table}) t")
        elif self._read_or_write == "write":
            db_schema_table = query_sql_or_table.split(".")
            self.spark_read_or_write \
                .option('dbschema', db_schema_table[0]) \
                .option('dbtable', query_sql_or_table)
            print(f"dbschema:{db_schema_table[0]},dbtable:{query_sql_or_table}")

        for key, value in options.items():
            self.spark_read_or_write.option(key, value)
        return self._load_or_save()

    def hive(self, query_sql_or_table, save_mode="auto", save_format="parquet", spark=None):
        """
        spark数据写入、读取hive的方式
        :param query_sql_or_table: sql or table
        :param save_mode: 选用存入方式 create insert auto(自动形式选择create or insert)
        :param save_format: 存入hive的数据格式
        :param spark: sparkSession对象
        :return:
        """
        if self._read_or_write == "read":
            return self._spark_or_df.sql(query_sql_or_table)
        elif self._read_or_write == "write":
            if save_mode == "create":
                self.spark_read_or_write.saveAsTable(query_sql_or_table, format=save_format)
            elif save_mode == "insert":
                self.spark_read_or_write.format(save_format) \
                    .insertInto(query_sql_or_table, overwrite=True if self._mode == "overwrite" else False)
            elif save_mode == "auto":
                databases = query_sql_or_table.split(".")[0]
                table_name = query_sql_or_table.split(".")[1]
                # 获取已存在的spark_session
                if spark is None:
                    spark = SparkSession.builder.getOrCreate()
                print(f"appName:{spark.sparkContext.appName},applicationId:{spark.sparkContext.applicationId}")
                table_list = [table.name for table in spark.catalog.listTables(databases)]
                if table_name in table_list:
                    print("insert {}".format(query_sql_or_table))
                    col_list = spark.sql(f"select * from {query_sql_or_table}").columns
                    print("列名顺序: {}".format(",".join(col_list)))
                    self.spark_read_or_write = self._spark_or_df.select(*col_list).write.mode(self._mode)
                    self.hive(query_sql_or_table, "insert", save_format)
                else:
                    print(f"save as table {query_sql_or_table}")
                    self.hive(query_sql_or_table, "create", save_format)

    def excel(self, df_or_file_path):
        if self._read_or_write == "read":
            pdf = pd.read_excel(df_or_file_path)
            return self._spark_or_df.createDataFrame(pdf)
        elif self._read_or_write == "write":
            return self._spark_or_df.toPandas().to_excel(df_or_file_path)
