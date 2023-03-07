"""
udf模块
"""
import pyspark.sql.functions as func

from pyspark.sql.types import DoubleType, LongType, DecimalType, StringType


class UDFFunc(object):
    """udf类"""

    @staticmethod
    @func.udf(returnType=DecimalType(precision=10, scale=2))
    def accuracy(target_value, pred):
        """
        Step1：计算每家门店每半小时的预测准确率，半小时1-MAPE=1-ABS(半小时实际单量-半小时预测单量)/半小时实际单量
        :param target_value:实际单量
        :param pred:预测单量
        :return:
        """
        return 1 - abs(target_value - pred) / target_value

    @staticmethod
    @func.udf(returnType=DecimalType(precision=10, scale=2))
    def abs_func(target_value, pred):
        """
        Step1：计算每家门店每半小时的绝对预测偏差值，半小时abs_error=ABS(半小时实际单量-半小时预测单量)
        :param target_value:实际单量
        :param pred:预测单量
        :return:
        """
        return abs(target_value - pred)

    @staticmethod
    @func.udf(returnType=DecimalType(precision=10, scale=2))
    def wmape_func(sum_abs, target_value):
        """
        每日1-WMAPE=1-SUM(半小时abs_error )/日实际总单量
        :param sum_abs:总误差绝对值
        :param target_value: 实际单量
        :return:
        """
        return 1 - sum_abs / target_value


