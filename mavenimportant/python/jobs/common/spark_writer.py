from pyspark.sql import DataFrameWriter

from jobs.common.config_init import ConfigInit
from jobs.utils.client_init import SparkInit
from jobs.utils.db_func import CommonDBFunc
from jobs.utils.password_utils import decrypt_password
from jobs.utils.read_write_func import SparkReaderWriter


class SparkWriter(SparkReaderWriter, DataFrameWriter):
    """兼容原始write函数"""

    def __init__(self, df, mode="overwrite"):
        """
        初始化
        :param df: dataframe
        :param mode:
        """
        SparkReaderWriter.__init__(self, df, mode)
        DataFrameWriter.__init__(self, df)
        self.mode(mode)

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
        user = gp_config["user"]
        password = gp_config["password"]
        # password = decrypt_password(gp_config["password"])
        url = db_func.get_db_url(ip, databases, db_name=db_name)
        print(url)
        print(f"user:{user},password:{password}")
        return super().relational_db(url, driver, user, password, query_sql_or_table, **options)

