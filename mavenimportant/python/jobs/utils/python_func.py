"""
python公共方法模块
"""
import os
import re
import calendar
import datetime
import subprocess


class CommonPythonFunc(object):
    """python相关的方法"""

    def __init__(self):
        self._configs_path_list = []

    def _recursion_path(self, _dict_path):
        """
        递归获取目录下所有文件
        :param _dict_path: 目录的路径
        :return:
        """
        if os.path.isdir(_dict_path):
            next_suffix = os.listdir(_dict_path)
            for index in range(0, len(next_suffix)):
                path = os.path.join(_dict_path, next_suffix[index])
                symbol = self._recursion_path(path)
                if index >= len(next_suffix):
                    return symbol
        else:
            self._configs_path_list.append(_dict_path)
            return False

    def path_from_dict(self, dict_path):
        """
        递归获取目录下所有文件
        :param dict_path: 目录的路径
        :return:
        """
        self._recursion_path(dict_path)
        configs_path_list = self._configs_path_list.copy()
        self._configs_path_list.clear()
        return configs_path_list

    @staticmethod
    def pattern_str_sub(resource_str, pattern, repl):
        """
        正则去除字符串中的字符
        :param resource_str: 原字符，
        :param pattern: 正则表达式
        :param repl: 将正则匹配到的字符替换
        :return:
        """
        re_p = re.compile(pattern)
        return re_p.sub(repl, resource_str)

    @staticmethod
    def pattern_str_findall(resource_str, pattern):
        """
        正则匹配字符串所有符合字符
        :param resource_str: 原字符，
        :param pattern: 正则表达式
        :return:
        """
        re_p = re.compile(pattern)
        return re_p.findall(resource_str)

    @staticmethod
    def get_format_time(time_str):
        """
        废除方法
        :param time_str:
        :return:
        """
        time_str = CommonPythonFunc.pattern_str_sub(time_str, '[^[0-9]]', '')
        if len(time_str) <= 4:
            time_format = "%Y"
        elif 4 < len(time_str) <= 6:
            time_format = "%Y%m"
        elif 6 < len(time_str) <= 8:
            time_format = "%Y%m%d"
        else:
            time_format = ""
        return time_format

    @staticmethod
    def get_head_or_end(time_str, time_format):
        """
        获取日期的月头和月尾数据
        :param time_str: 时间字符串
        :param time_format: 格式化正则
        :return:
        """
        time_month = datetime.datetime.strptime(time_str, time_format).month
        time_year = datetime.datetime.strptime(time_str, time_format).year
        max_day = calendar.monthrange(time_year, time_month)[1]
        start_month_time = datetime.date(year=time_year, month=time_month, day=1).strftime("%Y%m%d")
        end_month_time = datetime.date(year=time_year, month=time_month, day=max_day).strftime("%Y%m%d")
        return start_month_time, end_month_time

    def spilt_time_by_month(self, min_time, max_time):
        """
        获取一个时间范围中的所有月头和月尾
        :param min_time: 开始时间"20190927"
        :param max_time: 结束时间"20200927"
        :return:
        """
        range_month_day_list = []
        for time in range(int(min_time[:6]), int(max_time[:6]) + 1):
            time = str(time)
            if (int(time[-2:]) <= 12) & (int(time[-2:]) != 0):
                start_month_time, end_month_time = self.get_head_or_end(time, "%Y%m")
                if start_month_time < min_time:
                    start_month_time = min_time
                if end_month_time > max_time:
                    end_month_time = max_time
                range_month_day_list.append((start_month_time, end_month_time))
        return range_month_day_list

    def spilt_time_by_week(self, min_time, max_time):
        """
        获取一个时间范围中的所有周头和周尾
        :param min_time: 开始时间"20190927"
        :param max_time: 结束时间"20200927"
        :return:
        """
        if (len(min_time) != 8) & (len(max_time) != 8):
            raise TypeError("输入时间格式不对，请输入8为时间字符串")
        range_week_day_list = []
        start_time_week = min_time
        end_time_week = min_time
        while (end_time_week <= max_time) & (start_time_week <= max_time):
            start_time_week, end_time_week = self.get_head_or_end_week(start_time_week, "%Y%m%d")
            if start_time_week < min_time:
                if end_time_week > max_time:
                    range_week_day_list.append((min_time, max_time))
                else:
                    range_week_day_list.append((min_time, end_time_week))
            else:
                if end_time_week > max_time:
                    range_week_day_list.append((start_time_week, max_time))
                else:
                    range_week_day_list.append((start_time_week, end_time_week))
            start_time_week = self.get_old_day(end_time_week, "%Y%m%d", 1)
        return range_week_day_list

    @staticmethod
    def get_head_or_end_week(time_str, time_format):
        """
        获取日期的周头和周尾数据
        :param time_str: 时间字符串
        :param time_format: 格式化正则
        :return:
        """
        date = datetime.datetime.strptime(time_str, time_format)
        week = date.weekday() + 1
        start_time_week = (date - datetime.timedelta(days=week - 1)).strftime(time_format)
        end_time_week = (date + datetime.timedelta(days=(7 - week))).strftime(time_format)
        return start_time_week, end_time_week

    @staticmethod
    def get_old_day(time_str, time_format, day_range):
        """
        获取距离这个字符时间的+day_range的字符时间
        :param time_str:
        :param time_format:
        :param day_range:
        :return:
        """
        return (datetime.datetime.strptime(time_str, time_format)
                + datetime.timedelta(days=day_range)).strftime(time_format)

    @staticmethod
    def get_old_week(time_str, time_format, week_range):
        """
        获取距离这个字符时间的+day_range的字符时间
        :param time_str:
        :param time_format:
        :param week_range:
        :return:
        """
        return (datetime.datetime.strptime(time_str, time_format)
                - datetime.timedelta(weeks=week_range)
                + datetime.timedelta(days=1)).strftime(time_format)

    @staticmethod
    def get_date_diff_day(start_date, end_date, time_format='%Y-%m-%d'):
        """
        获取距离这个字符时间的+day_range的字符时间
        :param start_date:
        :param end_date:
        :param time_format:
        :return:
        """
        return (datetime.datetime.strptime(end_date, time_format)
                - datetime.datetime.strptime(start_date, time_format)).days

    @staticmethod
    def run_shell(_sh):
        print(_sh)
        response = subprocess.getstatusoutput(_sh)
        if response[0] != 0:
            raise RuntimeError(response[1])
        else:
            print("success")

    def json_lower_or_upper(self, json, symbol="lower"):
        """
        将json体中key全部转换成大写或小写
        :param json: 待转换的json
        :param symbol: 大写:upper, 小写:lower
        :return:
        """
        if not isinstance(json, dict):
            raise TypeError(f"json:{json} must dict")

        if symbol.lower() in ["lower", "upper"]:
            symbol = symbol.lower()
        else:
            raise TypeError(f"symbol:{symbol} must be lower or upper")

        return {key.lower() if symbol.lower() == "lower" else key.upper() if isinstance(key, str) else key
                : self.json_lower_or_upper(value, symbol) if isinstance(value, dict) else value
                for key, value in json.items()}

    @staticmethod
    def create_file(file_path):
        """
        创建文件
        :param file_path: excel文件地址
        :return:
        """
        if not os.path.exists(file_path):
            dir_path = os.path.dirname(file_path)
            if not os.path.exists(dir_path):
                print(f"创建目录：{dir_path}")
                os.makedirs(dir_path)
            print("创建excel文件")
            with open(file_path, "a+", encoding="utf_8") as read:
                print(read.read())
