"""
spark公共方法模块
"""
import datetime

import pyspark.sql.functions as func


class CommonSparkFunc(object):
    """spark相关的方法"""

    @staticmethod
    def date_to_format(_df, _cols):
        """
        对spark.dataframe的字符时间类型转换成时间格式
        :param _df: spark.dataframe
        :param _cols: 需要转换的列可以是list也可以是str
        :return:
        """
        cols = _df.columns
        if isinstance(_cols, str):
            return _df.withColumn(_cols, func.to_date(_df[_cols], "yyyyMMdd"))
        elif isinstance(_cols, list):
            for col in _cols:
                _df = _df.withColumn(col, func.to_date(_df[col], "yyyyMMdd"))
            return _df.select(cols)
        else:
            raise TypeError("Please input the correct format")

    @staticmethod
    def time_zone(_df, col_name, time_pattern="yyyy-MM-dd", time_zone="CST"):
        """
        转时区，mysql存在
        :param _df:
        :param col_name:
        :param time_pattern:
        :param time_zone:
        :return:
        """
        return _df.withColumn(col_name, func.to_utc_timestamp(func.to_date(col_name, time_pattern), time_zone))

    @staticmethod
    def get_json_object_after(_df, json_str_col):
        """
        对get_json_object的json_str规范化
        :param _df:
        :param json_str_col: 需要规范化的json_str
        :return:
        """
        return _df.withColumn(json_str_col,
                              func.regexp_replace(
                                  func.regexp_replace(
                                      func.regexp_replace(
                                          func.regexp_replace(
                                              func.regexp_replace(
                                                  func.regexp_replace(
                                                      func.regexp_replace(
                                                          func.regexp_replace(
                                                              _df[json_str_col],
                                                              "\\{'", "\\{\""),
                                                          "'\\}", "\"\\}"),
                                                      "': '", "\": \""),
                                                  "', '", "\", \""),
                                              ": '", ": \""),
                                          ", '", ", \""),
                                      "', ", "\", "),
                                  "': ", "\": "))

    @staticmethod
    def date_to_str(_df, especially_types):
        """
        转换spark.dataframe中日期字段为字符
        :param _df:
        :param especially_types: 需要转换的类型 str or list
        :return:
        """
        if isinstance(especially_types, str):
            especially_types = [especially_types]
        for col_name, types in _df.dtypes:
            if types in especially_types:
                _df = _df.withColumn(col_name, _df[col_name].cast("string"))
        return _df

    @staticmethod
    def df_cols_rename(_df, cols_name, col_suffix):
        """

        :param _df:
        :param cols_name:
        :param col_suffix:
        :return:
        """
        if isinstance(cols_name, str):
            cols_name = [cols_name]
        for col_name in cols_name:
            _df = _df.withColumnRenamed(col_name, f"{col_name}_{col_suffix}")
        return _df

    @staticmethod
    def spark_numerical_accurate(_df):
        """
        转换spark.dataframe中日期字段为字符
        :param _df:
        :return:
        """
        numerical_type = ["int"]
        for col_name, types in _df.dtypes:
            if types in numerical_type:
                _df = _df.withColumn(col_name, _df[col_name].cast("string"))
        return _df

    @staticmethod
    def spark_to_pandas(_df):
        """
        解决spark转Pandas数据类型丢失
        :param _df:
        :return:
        """
        pdf = _df.toPandas()
        for col_tuple in _df.dtypes:
            col_type = col_tuple[1]
            if (col_type in ("double", "float")) or ("decimal" in col_type):
                pdf[col_tuple[0]] = pdf[col_tuple[0]].astype(float)
            elif "int" in col_type:
                try:
                    pdf[col_tuple[0]] = pdf[col_tuple[0]].astype(int)
                except:
                    pdf
            elif (col_type in ("string", "date", "timestamp")) or ("varchar" in col_type):
                pdf[col_tuple[0]] = pdf[col_tuple[0]].astype("string")
            elif "datetime" in col_type:
                pdf[col_tuple[0]] = pdf[col_tuple[0]].astype(datetime)
            elif "binary" in col_type:
                pdf
            else:
                print(col_tuple)
                raise Exception(pdf.dtypes)
        return pdf