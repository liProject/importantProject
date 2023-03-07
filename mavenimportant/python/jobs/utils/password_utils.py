import subprocess

"""
二进制密码加密，解密程序详见 git项目  http://10.92.218.69/rainbow_arp/arp_crypto
"""


def is_windows():
    import sys
    return sys.platform.startswith('win')


def decrypt_password(encrypted_password: str):
    """
    使用二进制密码程序对密码加密字符串进行解密
    """
    password_command = 'password'
    cmd = subprocess.Popen(f'{password_command} decrypt {encrypted_password}', stdin=subprocess.PIPE,
                           stderr=subprocess.PIPE,
                           stdout=subprocess.PIPE, universal_newlines=True, shell=True, bufsize=1)
    return '\n'.join(cmd.stdout.readlines())


def encrypt_password(decrypted_password: str):
    """
    使用二进制密码程序对密码明文字符串进行加密
    """
    password_command = 'password'
    cmd = subprocess.Popen(f'{password_command} encrypt {decrypted_password}', stdin=subprocess.PIPE,
                           stderr=subprocess.PIPE,
                           stdout=subprocess.PIPE, universal_newlines=True, shell=True, bufsize=1)
    stderr_message = '\n'.join(cmd.stderr.readlines())
    if stderr_message.strip() != '':
        raise Exception(stderr_message)
    return '\n'.join(cmd.stdout.readlines())
