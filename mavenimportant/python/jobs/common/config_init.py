# -*- coding:utf8 -*-
"""
配置文件模块
"""
import json
import os

from jobs.utils.python_func import CommonPythonFunc


class ConfigInit(object):
    """
    配置文件初始化类
    """

    @staticmethod
    def _get_config_dict():
        """
        获取configs目录下所有文件路径
        :return:
        """
        configs_dict = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "configs")
        if os.path.isdir(configs_dict):
            return CommonPythonFunc().path_from_dict(configs_dict)
        else:
            print("No file exists under {}".format(configs_dict))

    def read_default_json(self):
        """
        读取默认配置文件
        :return:
        """
        configs_list = self._get_config_dict()
        default_path = [configs_path for configs_path in configs_list if configs_path.endswith("default.json")][0]
        with open(default_path, "r", encoding="utf_8") as read:
            return json.loads(read.read())

    def read_scene_json(self, scene_name):
        """
        读取场景配置文件
        :param scene_name: 场景配置文件名称
        :return:
        """
        configs_list = self._get_config_dict()
        scene_path = [configs_path for configs_path in configs_list
                      if configs_path.endswith(f"{os.sep}{scene_name}.json")][0]
        with open(scene_path, "r", encoding="utf_8") as read:
            return json.loads(read.read())

    def read_python_properties(self, scene_name):
        """
        读取场景配置文件
        :param scene_name: 场景配置文件名称
        :return:
        """
        configs_list = self._get_config_dict()
        scene_path = [configs_path for configs_path in configs_list
                      if configs_path.endswith(f"{os.sep}{scene_name}.properties")][0]
        with open(scene_path, "r", encoding="utf_8") as read:
            text_list = read.readlines()
            properties_dict = {}
            for text in text_list:
                key_value = text.replace("\n", "").split("=")
                if len(key_value) == 2:
                    properties_dict[key_value[0].strip()] = key_value[1].strip()
        return properties_dict

    @staticmethod
    def get_java_resource_dir():
        """
        获取java工程配置文件目录
        :return:
        """
        python_path = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
        resources_config_path_suffix = "src/main/resources"
        return os.path.join(os.path.dirname(python_path), resources_config_path_suffix)

    def get_db_json(self):
        """
        返回Java工程下数据库json配置文件数据
        :return:
        """
        resources_config_path = os.path.join(self.get_java_resource_dir(), "globalConf.json")
        with open(resources_config_path, "r", encoding="utf_8") as read:
            return json.loads(read.read())

    def get_properties(self, file_name):
        """
        获取java工程配置文件目录下properties数据
        :param file_name: 需要读取的properties名字
        :return:
        """
        resources_config_path = os.path.join(self.get_java_resource_dir(), file_name)
        with open(resources_config_path, "r", encoding="utf_8") as read:
            text_list = read.readlines()
            properties_dict = {}
            for text in text_list:
                key_value = text.replace("\n", "").split("=")
                if len(key_value) == 2:
                    properties_dict[key_value[0].strip()] = key_value[1].strip()
        return properties_dict


if __name__ == '__main__':
    config_init = ConfigInit()
    print(config_init.read_scene_json("starbucks_email_config"))
