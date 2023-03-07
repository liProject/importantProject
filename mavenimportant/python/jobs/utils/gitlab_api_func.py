"""
统计gitlab下代码量的模块
"""
import requests
from urllib3.exceptions import InsecureRequestWarning


class GitlabApiFunc(object):
    """统计gitlab下代码量的工具"""

    def __init__(self, private_token, ip, port=80):
        """
        初始化
        :param private_token: gitlab访问令牌
        :param ip: gitlab的ip地址
        :param port: gitlab的端口号，默认是80 or 8080
        """
        self._private_token = private_token
        self._url_prefix = f"http://{ip}:{port}/"
        requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

    def get_project_all(self):
        """
        获取该private_token下所有项目信息
        :return:
        """
        page = 1
        project_list = []
        # 一次只能提取20条项目记录
        while True:
            response_json_list = requests.get(f"{self._url_prefix}api/v4/projects/"
                                              f"?page={page}&private_token={self._private_token}").json()
            if len(response_json_list) == 0:
                break
            for response_json in response_json_list:
                project_list.append({"project_id": response_json["id"], "project_name": response_json["name"]})
            page += 1
        return project_list

    def get_branch_by_project(self, project_id):
        """
        获取项目下所有commit信息（不区分分支）
        :param project_id: 项目id
        :return:
        """
        page = 1
        branch_list = []
        # 一次只能提取20条项目记录
        while True:
            response_json_list = requests.get(f"{self._url_prefix}api/v4/projects/{project_id}/repository/branches"
                                              f"?page={page}&private_token={self._private_token}").json()
            if len(response_json_list) == 0:
                break
            for response_json in response_json_list:
                branch_list.append({"branch_name": response_json["name"],
                                    "branch_author_name": response_json["commit"]["author_name"]})
            page += 1
        return branch_list

    def get_all_commit_by_branch(self, project_id, branch_name):
        """
        通过branch_name获取该分支下所有commit信息
        :param project_id: 项目id
        :param branch_name: 分支名
        :return:
        """
        page = 1
        commit_list = []
        # 一次只能提取20条项目记录
        while True:
            response_json_list = requests.get(f"{self._url_prefix}api/v4/projects/{project_id}/repository/commits"
                                              f"?ref_name={branch_name}&page={page}"
                                              f"&private_token={self._private_token}").json()
            if len(response_json_list) == 0:
                break
            for response_json in response_json_list:
                commit_list.append({"commit_id": response_json["id"],
                                    "commit_author_name": response_json["author_name"],
                                    "commit_date": response_json["authored_date"]})
            page += 1
        return commit_list

    def get_code_cun_by_commit(self, project_id, commit_id):
        """
        通过commit_id获取该commit_id的代码提交量
        :param project_id: 项目id
        :param commit_id: 代码提交id
        :return:
        """
        response_json_list = requests.get(
            f"{self._url_prefix}api/v4/projects/{project_id}/repository/commits/{commit_id}"
            f"?private_token={self._private_token}").json()
        print(response_json_list)
        return {"additions": response_json_list["stats"]["additions"],
                "deletions": response_json_list["stats"]["deletions"], "total": response_json_list["stats"]["total"]}


if __name__ == '__main__':
    gitlab_client = GitlabApiFunc("dpqxos3Cn4wbAbcjgY-u", "10.92.218.69", 80)
    print(gitlab_client.get_code_cun_by_commit(55, "eeb7210e928c7eedd846a3c96c423396e24e458b"))
