from pyspark import HiveContext
from pyspark.sql import DataFrameReader

from jobs.common.config_init import ConfigInit
from jobs.utils.db_func import CommonDBFunc
from jobs.utils.read_write_func import SparkReaderWriter


class SparkReader(SparkReaderWriter, DataFrameReader):
    """兼容原始read函数"""

    def __init__(self, spark_session):
        """
        初始化
        :param spark_session: SparkSession
        """
        SparkReaderWriter.__init__(self, spark_session, None)
        DataFrameReader._jreader = HiveContext(sparkContext=spark_session.sparkContext)._ssql_ctx.read()
        DataFrameReader._spark = spark_session

    def relational_db(self, db_name, database_env, databases, query_sql_or_table, **options):
        """

        :param db_name:
        :param database_env:
        :param databases:
        :param query_sql_or_table:
        :param options:
        :return:
        """
        db_func = CommonDBFunc()
        driver = db_func.get_db_driver(db_name)
        gp_config = ConfigInit().read_python_properties(f"{db_name}_{database_env}")
        ip = gp_config["ip"]
        user = gp_config["read_user"]
        password = gp_config["read_password"]
        url = db_func.get_db_url(ip, databases, db_name=db_name)
        return super().relational_db(url, driver, user, password, query_sql_or_table, **options)
