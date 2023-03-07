"""
爬虫公共方法模块
"""
import requests


class CommonNetworkRequestFunc(object):
    """爬虫相关的方法"""

    @staticmethod
    def get_session_id():
        """
        获取会话id
        Returns
        -------

        """
        data = {
            'username': "admin",
            'password': "rainbow123",
            'action': 'login'
        }
        return requests.post("https://10.92.210.132:8443/", data=data, verify=False).json().get("session.id")


if __name__ == '__main__':
    """
    curl -k --data "execid=5684&lastUpdateTime=-1&session.id=c583dd1c-2048-4ca4-98bb-f3da0a24f404" https://10.92.210.132:8443/executor?ajax=fetchexecflowupdate
    """

    all_job_status = requests.get(
        "https://10.92.210.132:8443/executor?ajax=fetchexecflowupdate&execid=5684&lastUpdateTime=-1&session.id=c583dd1c-2048-4ca4-98bb-f3da0a24f404",
        verify=False).json()
    all_job_status_dict = {}
    all_job_status_dict["flow_status"] = all_job_status["status"]
    all_job_status_dict["job_status"] = []
    for job_dict in all_job_status["nodes"]:
        all_job_status_dict["job_status"].append({job_dict["id"]: job_dict["status"]})
    print(all_job_status_dict)
