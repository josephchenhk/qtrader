# -*- coding: utf-8 -*-
# @Time    : 13/9/2020 3:29 PM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: bondsim_client.py
# @Software: PyCharm
import os
import ast
import socket
from datetime import datetime
import pytz
import json
import time
from datetime import timedelta
import threading

from qtrader.core.utility import main_path

os.chdir(main_path)

with open(f'setting/connect_bondsim.json') as json_file:
    setting = json.load(json_file)
    HOST = setting["地址"]
    PORT = setting["端口"]

class SimulatorClient:

    HOST = HOST  # The server's hostname or IP address
    PORT = PORT  # The port used by the server

    def __init__(self):
        self.handlers = {}
        self._init_socket()
        print("启动仿真器客户端")

    def _init_socket(self):
        """ 接入socket"""
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.s.connect((self.HOST, self.PORT))
        except ConnectionRefusedError as e:
            raise ConnectionError("Socket client fails to start because {}.".format(e))

    def start(self):
        """ 启动线程接收消息"""
        t = threading.Thread(target=self.start_consuming)
        t.setDaemon(True)
        t.setName("SimClientThread")
        t.start()

    def start_consuming(self):
        """ 处理消息"""
        data_all = ''
        while True:
            streamBytes = self.s.recv(1024)  # streamBytes是可能被任意截断后的stream  byte
            data_all += streamBytes.decode()  # str
            _index = data_all.find('\n')
            while _index != -1 and len(data_all) > 1:  # len(data_all)>1 是因为避免data_all='\n'时导致报错
                data, others = data_all[:_index], data_all[_index+1:]  # 如果包含换行符 换行符前的data数据必然是完整的
                data_dict = ast.literal_eval(data)
                if data_dict.get('type') == "SNAPSHOT":
                    ss = data_dict.get("data")
                    # print("Receive", ss)
                    self.handlers["QUOTE"].on_recv_rsp(ss)
                    self.handlers["ORDERBOOK"].on_recv_rsp(ss)
                data_all = others  # 处理后续数据
                _index = data_all.find('\n')

    def set_handler(self, handler):
        """ 接受handler 实例，以其中的 on_recv_rsp(self, rsp_str) 函数作为回调函数"""
        name = handler.name
        self.handlers[name] = handler

    def send_timer_to_server(self, tic):
        """ 将当前时间发送给服务器端，促使服务器发布最新市场数据快照（主动等候一段时间以避免socket broken）"""
        timer = {"type": "TIMER", "data": tic.isoformat()}
        self.s.sendall(bytes(json.dumps(timer), 'utf-8'))
        time.sleep(2)

if __name__=="__main__":

    start = datetime(2020, 6, 19, 8, 0, 0)
    end = datetime(2020, 6, 23, 7, 59, 59)
    tz = pytz.timezone("Asia/Hong_Kong")
    start = tz.localize(start)
    end = tz.localize(end)
    print(start, end)

    sc = SimulatorClient()

    tic = start
    while True:
        timer = {"type":"TIMER", "data":tic.isoformat()}
        sc.s.sendall(bytes(json.dumps(timer), 'utf-8'))
        # data = s.recv(1024)            # recv是block函数，直到接收到数据以后才会运行（收到server传回的信息以后才会继续运行）
        # print('Received', repr(data))
        time.sleep(2)
        tic += timedelta(seconds=1)