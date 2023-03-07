"""
数据库公共方法模块
"""


class CommonDBFunc(object):
    """数据库相关的方法"""

    def __init__(self, default_db_params=None):
        """

        :param default_db_params: db_name key值必须是小写的
        """
        self._default_db_params = default_db_params if default_db_params else {
            "gp": {
                "port": 5432,
                "driver": "org.postgresql.Driver",
                "host": "postgresql"
            },
            "mysql": {
                "port": 3306,
                "driver": "com.mysql.jdbc.Driver",
                "host": "mysql"
            }}

    def get_db_url(self, ip, databases, db_name=None, port=None, host=None):
        """
        获取关系型数据库url
        :param ip: ip地址
        :param databases: 数据库
        :param db_name: 关系型数据名称
        :param port: 数据库端口号
        :param host: 数据库的host
        :return:
        """
        if db_name:
            print(self._default_db_params)
            port = self._default_db_params[db_name.lower()]["port"] if not port else port
            host = self._default_db_params[db_name.lower()]["host"] if not host else host
        elif (port is None) or (host is None):
            raise TypeError(f"db_name:{db_name} and port:{port} and host:{host} must not null")
        return f"jdbc:{host}://{ip}:{port}/{databases}"

    def get_db_driver(self, db_name):
        """
        获取数库的driver
        :param db_name: 数据库名称
        :return:
        """
        return self._default_db_params[db_name.lower()]["driver"]
