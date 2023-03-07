import os
import math

import matplotlib.pyplot as plt

from pyspark.sql import DataFrame

from jobs.utils.client_init import SparkInit
from jobs.utils.spark_func import CommonSparkFunc


class DrawingPicture(object):

    @staticmethod
    def drawings_by_df(df, y_cols, x_col, title, y_name):
        """
        画图方法
        不需要点，则去掉marker='o'
        :param df:
        :param y_cols:
        :param x_col:
        :param title:
        :param y_name:
        :return:
        """
        if isinstance(df, DataFrame):
            df = df.toPandas()

        if isinstance(y_cols, str):
            y_cols = [y_cols]

        drawing_df = df.sort_values(by=x_col)
        for y_col in y_cols:
            plt.plot(drawing_df[x_col], drawing_df[y_col], '-', marker='o', label=y_col)
        plt.title(title)
        plt.ylabel(y_name)
        plt.legend()

    def drawings_by_group_df(self, df, y_cols, x_col, title, y_name, plt_col_num, dic_path=None, group_cols=None):
        """
        画图方法
        :param df:
        :param y_cols:
        :param x_col:
        :param title:
        :param y_name:
        :param plt_col_num:
        :param dic_path:
        :param group_cols:
        :return:
        """
        plt.rcParams['font.sans-serif'] = [u'SimHei']
        plt.rcParams['axes.unicode_minus'] = False
        if group_cols:
            if isinstance(group_cols, str):
                group_cols = [group_cols]
            group_df = df.dropDuplicates(subset=group_cols).select(*group_cols)
            group_df_cols = group_df.dtypes
            where_on_dict = {}
            for row in group_df.collect():
                temp_where_list = []
                value_title_list = []
                for index in range(0, len(group_df_cols)):
                    col_name = group_df_cols[index][0]
                    col_type = group_df_cols[index][1]
                    col_value = row[index]
                    if (col_type in ("string", "date", "timestamp")) or ("varchar" in col_type) or (
                            "datetime" in col_type):
                        col_value = f"'{col_value}'"
                    temp_where_list.append(f"{col_name} = {col_value}")
                    value_title_list.append(col_value.replace("'", ""))
                where_on_dict[" and ".join(temp_where_list)] = "_".join(value_title_list)
            plt_row_num = math.ceil(len(where_on_dict) / 3)
            plt.figure(figsize=(plt_col_num * 10, plt_row_num * 6))
            i = 1
            for where_on, where_title in where_on_dict.items():
                drawings_df = df.where(where_on).persist()
                plt.subplot(plt_row_num, plt_col_num, i)
                self.drawings_by_df(drawings_df, y_cols, x_col, f"{where_title}_{title}", y_name)
                i += 1
        else:
            plt.figure(figsize=(10, 6))
            self.drawings_by_df(df, y_cols, x_col, title, y_name)

        if dic_path is not None:
            plt.savefig(os.path.join(dic_path, f"{title}.png"))
        else:
            plt.show()

    @staticmethod
    def drawings_by_pandas(pdf, x_col, y_cols, title, save_path=None, **params):
        """
        使用pandas自带得画图，跟家简便
        :param pdf: pandas.DataFrame or spark.sql.DataFrame
        :param x_col: x轴
        :param y_cols: y轴，可以是多个
        :param title: 图片名称和标题
        :param save_path: 保存路径，不传入或传入None，都不保存
        :param params:
            figsize: 图片尺寸
            rot:
            secondary_y: 是否开启第二y轴, list
            axv_line_index: 是否使用虚线标识具体x轴数据
        :return:
        """
        if isinstance(pdf, DataFrame):
            pdf = CommonSparkFunc().spark_to_pandas(pdf)

        if isinstance(y_cols, str):
            y_cols = [y_cols]

        pdf.sort_values(x_col).set_index(x_col)[y_cols] \
            .plot(figsize=params.get("figsize", (18, 8)), rot=params.get("rot", 30),
                  secondary_y=params.get("secondary_y", None), title=title)

        for index in params.get("axv_line_index", []):
            plt.axvline(index, linestyle='--', color='g')

        for index_value in params.get("axv_line_index_values", []):
            index_list = pdf[pdf[params.get("axv_line_index_col", '')] == index_value].index.tolist()
            if len(index_list) > 0:
                plt.axvline(index_list[0], linestyle='--', color='g')

        if save_path:
            plt.savefig(save_path)
        else:
            plt.show()


if __name__ == '__main__':
    spark = SparkInit().appName.getOrCreate()
    error_group = {1: [1, 3, 5]}
    for index in range(0, 10, 1):
        if error_group.get(index) is not None and index in error_group.get(index):
            print(f"index:{index} 跳过")
            continue
        print(f"index:{index} 不跳过")