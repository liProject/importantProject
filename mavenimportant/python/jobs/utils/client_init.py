"""
客户端初始模块
"""
import re
import sys

from pyspark.sql import SparkSession


class SparkInit(SparkSession.Builder):
    """spark创建对象，适合多输入、输出源场景"""

    def __init__(self):
        """
        初始化
        """
        file_path = sys.argv[0]
        re_p = re.compile("([^/]*)\.py")
        app_name = re_p.findall(file_path)[0]
        self._builder = SparkSession.builder.appName(app_name)

    def hive(self, metastore_ip=None):
        """
        创建能读取hive的spark对象(支持动态分区覆盖)
        :return:
        """
        if metastore_ip:
            self._builder.config("hive.metastore.uris", f"thrift://{metastore_ip}:9083")
        self._builder \
            .config("hive.exec.dynamic.partition", True) \
            .config("hive.exec.dynamic.partition.mode", "nonstrict") \
            .config("spark.sql.catalogImplementation", "hive") \
            .config("spark.sql.hive.convertMetastoreOrc", False) \
            .config("spark.sql.hive.convertMetastoreParquet", False) \
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        return self

    def mysql(self, time_zone=None):
        """
        创建读取mysql带有时区的
        :param time_zone:
        :return:
        """
        if time_zone is not None:
            self._builder.config('spark.sql.session.timeZone', time_zone)
        return self

    def save_parquet(self):
        """
        spark写入hive使用parquet
        true 数据会以Spark1.4和更早的版本的格式写入。比如decimal类型的值会被以Apache Parquet的fixed-length byte array格式写出，
                该格式是其他系统例如Hive、Impala等使用的。
        False 会使用parquet的新版格式。例如，decimals会以int-based格式写出。
                如果Spark SQL要以Parquet输出并且结果会被不支持新格式的其他系统使用的话，需要设置为true。
        :return:
        """
        self._builder \
            .config("spark.sql.parquet.writeLegacyFormat", True)
        return self

    def hive_regex(self):
        """
        hive使用正则，非必要不要加上，防止异常正则出现
        :return:
        """
        self._builder \
            .config("hive.support.quoted.identifiers", None) \
            .config("spark.sql.parser.quotedRegexColumnNames", True)
        return self

    def get_or_create(self):
        """
        获得spark对象
        :return:
        """
        spark = self._builder.config('spark.sql.execution.arrow.enabled', "false").getOrCreate()
        print(f"appName:{spark.sparkContext.appName},applicationId:{spark.sparkContext.applicationId}")
        return spark

