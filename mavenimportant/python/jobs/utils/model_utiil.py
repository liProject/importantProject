import lightgbm as lgb
import matplotlib.pyplot as plt
from lightgbm import plot_importance

from mmlspark.lightgbm import LightGBMRegressionModel


class ModelUtil(object):

    @staticmethod
    def model_readability(input_model_path, out_model_path):
        """
        解析模型文件，达到模型文件可读化
        :param input_model_path: 输入模型文件（hdfs）
        :param out_model_path: 输出模型文件（hdfs）
        :return:
        """
        model = LightGBMRegressionModel.load(input_model_path)
        model.saveNativeModel(out_model_path, overwrite=True)

    @staticmethod
    def drawings_model_feature_weight(model_file, picture_path=None):
        """
        绘制模型的特征权重，也可以直接解析模型文件
        :param model_file:本地的模型文件
        :param picture_path:需要将图片保存到何处
        :return:
        """
        model = lgb.Booster(model_file=model_file)
        fig, ax = plt.subplots(figsize=(25, 15))
        plot_importance(model, height=0.5, ax=ax, max_num_features=64)
        plt.title("feature_weight")
        if picture_path:
            plt.savefig(picture_path)
        else:
            plt.show()

    @staticmethod
    def get_model_feature_weight_by_model(model):
        """
        根据sklearn的模型获取特征权重
        :param model: sklearn.lightgbm
        :return:
        """
        booster = model.booster_
        importance = booster.feature_importance(importance_type='split')
        feature_name = booster.feature_name()
        return feature_name, importance
