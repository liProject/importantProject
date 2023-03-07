import os
import sys

from jobs.utils.python_func import CommonPythonFunc


class EolChange(object):
    
    @staticmethod
    def eol_change(_file_path):
        """
        转换文件格式
        :param _file_path: 文件名
        :return:
        """
        with open(_file_path, "r", encoding="utf_8") as read:
            new_lines = read.readlines()
        with open(_file_path, "w", encoding="utf_8") as write:
            for line in new_lines:
                write.write(line)

    def run(self, _dict_path):
        """
        主入口
        :param _dict_path: 目录地址
        :return:
        """
        all_file_paths = CommonPythonFunc().path_from_dict(_dict_path)
        for path in all_file_paths:
            if path.endswith(".sh"):
                print(path)
                self.eol_change(path)


if __name__ == '__main__':
    if len(sys.argv) >= 2:
        for index in range(1, len(sys.argv)):
            dict_path = sys.argv[index]
            print(dict_path)
            EolChange().run(dict_path)
    else:
        raise TypeError("没有传入目录")



