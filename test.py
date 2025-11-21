# 运行目录：在项目根目录下运行
import subprocess
import time
import os
import io
import sys
import atexit
import paramiko
import matplotlib.pyplot as plt
import numpy as np
import json
import threading
import logging
import sys

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)

# sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
max_try = 5
workspace = os.getcwd()
output = workspace + "/build/output.txt"
result = workspace + "/result.txt"
figure_path = ""

def kill_server():
    with open(output, "w", encoding="utf-8") as outfile:
        subprocess.run("ps -ef | grep smallbank_test | grep -v grep | awk '{print $2}' | xargs kill -9",stdout=outfile, stderr=outfile,shell=True)
        subprocess.run("ps -ef | grep compute_node | grep -v grep | awk '{print $2}' | xargs kill -9",stdout=outfile, stderr=outfile,shell=True)
        subprocess.run("rm ./output.txt", stdout=outfile, stderr=outfile, shell=True)
    time.sleep(1)

def build():
    with open(output, "w", encoding="utf-8") as outfile:
        subprocess.run("rm -rf ./build", stdout=outfile, stderr=outfile, shell=True)
        subprocess.run("mkdir ./build", stdout=outfile, stderr=outfile, shell=True)
        subprocess.run("cd ./build && cmake ..", stdout=outfile, stderr=outfile, shell=True)
        subprocess.run("cd ./build && make -j8", stdout=outfile, stderr=outfile, shell=True)
    time.sleep(1)
    


username = 'root'
password = '20001010@@HcY'

# run settings
# const
RunWorkloadTpye = ['smallbank', 'tpcc']
StorageWorkloadType = ["SmallBank", "TPCC"]
RunModeType = [0, 4, ]
SmallBank_TX_NAME = ["Amalgamate", "Balance", "DepositChecking", "SendPayment", "TransactSaving", "WriteCheck"]
TPCC_TX_NAME = ["NewOrder", "Payment", "Delivery", "OrderStatus", "StockLevel"]

# dynamic
ComputeNodeNum = 1
ComputeNodeNUmVec = [8]
workload_type = 0 # 0: smallbank, 1: tpcc
workload_type_Vec1 = [0]
workload_type_Vec2 = [1]
ReadOnlyRatio1 = [0]
ReadOnlyRatio2 = [0.08]
localTxnRatio1 = [0.5]
localTxnRatio2 = [0.1,0.3,0.5,0.7,0.9]
WrongRatio = [0]
RunLongTxn = True  # 是否运行长事务实验

if __name__ == "__main__":

    # 删除之前的结果
    if os.path.exists(result):
        os.remove(result)
    # 创建图像文件夹
    if not os.path.exists("./result"):
        os.mkdir("./result")
    os.chdir("./result")
    # 创建此次测试的结果文件夹，以时间命名
    time_str = time.strftime("%Y%m%d%H%M%S", time.localtime())
    os.mkdir(time_str)
    figure_path = os.path.join(os.getcwd(), time_str)

    # !开始本次的测试
    os.chdir(workspace)
    # atexit.register(cleanup, 'node1')
    
    # delay_time_list = [300, 500, 800, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000]
    delay_time_list1 = [5000]
    delay_time_list2 = [500]
    coro_num_list = [16]
    hot_num_list1 = [300000]
    hot_num_list2 = [5000]
    long_txn_rate_list = [0, 0.05, 0.10, 0.15, 0.20, 0.25] #长事务比例0, 0.05
    epoch_time_list1 = [100]
    epoch_time_list2 = [100]
    set_system_MAX_ITEM_SIZE(0)    # 0是SmallBank 1是TPC-C
    set_system_LongTxn(RunLongTxn) # 0是关闭长事务，1是开启长事务
    set_system_UniformHot(0) # 0关闭均匀热点分布设置, 1开启均匀热点分布设置

    for wrong_num in WrongRatio:
        set_system_wrong(wrong_num)
        for delay_time in delay_time_list1:
            set_system_delay(delay_time)

            for ComputeNodeNum in ComputeNodeNUmVec:
                for coro_num in coro_num_list:
                    for workload_type in workload_type_Vec1:
                        for mode in RunModeType1:
                            for read_only_ratio in ReadOnlyRatio1:
                                for local_txn_ratio in localTxnRatio1:
                                    for long_txn_rate in long_txn_rate_list:
                                        # !修改配置文件
                                        # 修改storage和remote server的配置文件
                                        ConfigStorageAndRemoteServer()

                                        # 遍历每个计算节点, 注意只需要配置指定节点数量的计算节点数
                                        for i in range(ComputeNodeNum):
                                            # 配置计算节点
                                            ConfigComputeServer(i)
                                            subprocess.run(['sshpass', '-p', password, 'scp', '-P', str(ssh_ports[i]), Code_Folder + '/config/compute_node_config.json', username + '@' + ssh_hostnames[i] + ':' + RemoteCodeFolder +'/config'], check=True)
                                            print('Update compute_node_config.json to remote' + ssh_hostnames[i])
                                            # config_outputs1 = ConfigeModify(RemoteCodeFolder+'/config/compute_node_config.json', len(ssh_hostnames), ssh_hostnames[i])

                                        time.sleep(5)

                                        process3 = subprocess.run(["docker", "exec", "node1", "/bin/bash", "-c", "pgrep -f remote_node"])
                                        process4 = subprocess.run(["docker", "exec", "node1", "/bin/bash", "-c", "pgrep -f storage_pool"])

                                        # !启动存储节点和远程节点
                                        storage_process = subprocess.Popen(['docker', 'exec', '-it', 'node1', '/bin/bash', '-c', 'cd ' + Docker_Build_Folder + '/storage_server ' + '&& ./storage_pool'])
                                        remote_process = subprocess.Popen(['docker', 'exec', '-it', 'node1', '/bin/bash', '-c', 'cd ' + Docker_Build_Folder + '/remote_server ' + '&& ./remote_node'])

                                        time.sleep(40) # 等待存储节点和远程节点启动完成

                                        # !启动计算节点
                                        threads.clear()
                                        commands.clear()
                                        for i in range(ComputeNodeNum):
                                            if RunLongTxn:
                                                commands.append([
                                                    "docker exec -i node" + str(i+2) + " /bin/bash -c 'pkill -f compute_server'",
                                                    "docker exec -i node" + str(i+2) + " /bin/bash -c 'cd " + Docker_Build_Folder + "/compute_server && ./compute_server " \
                                                        + RunWorkloadTpye[workload_type] + " " + mode +" 12 " + str(coro_num) + " "+ str(read_only_ratio) + " " + str(local_txn_ratio) + " " + str(long_txn_rate) + " > " + Docker_Build_Folder + "/output.txt'",
                                                ])
                                            else:
                                                commands.append([
                                                    "docker exec -i node" + str(i+2) + " /bin/bash -c 'pkill -f compute_server'",
                                                    "docker exec -i node" + str(i+2) + " /bin/bash -c 'cd " + Docker_Build_Folder + "/compute_server && ./compute_server " \
                                                        + RunWorkloadTpye[workload_type] + " " + mode +" 12 " + str(coro_num) + " "+ str(read_only_ratio) + " " + str(local_txn_ratio) + " > " + Docker_Build_Folder + "/output.txt'",
                                                ])
                                            thread = threading.Thread(target=ssh_execute_command, args=(ssh_hostnames[i], ssh_ports[i], username, password, commands[i]))
                                            threads.append(thread)
                                            thread.start()
                                            time.sleep(3)

                                        for thread in threads:
                                            thread.join()

                                        print("All compute nodes finish running")
                                        # !获取结果
                                        result_data = []
                                        for i in range(ComputeNodeNum):
                                            mkdir_pass = figure_path + '/' + RunWorkloadTpye[workload_type] + "_" + mode + "_12_" + str(coro_num) + "_" + str(hot_num) + "_" + str(epoch_time) + "_" + str(read_only_ratio) + "_" + str(local_txn_ratio) + "_" + str(ComputeNodeNum) + "_" + str(delay_time) + "_" + str(wrong_num) + "_" + str(K) + "_" + str(H) + "_" + str(long_txn_rate) + "/node" + str(i+2)
                                            subprocess.Popen(['mkdir', '-p', mkdir_pass])
                                            subprocess.run(['sshpass', '-p', password, 'scp', '-P', str(ssh_ports[i]), username + '@' + ssh_hostnames[i] + ':' + RemoteCodeFolder + '/build/compute_server/result.txt', mkdir_pass], check=True)
                                            subprocess.run(['sshpass', '-p', password, 'scp', '-P', str(ssh_ports[i]), username + '@' + ssh_hostnames[i] + ':' + RemoteCodeFolder + '/build/compute_server/delay_fetch_remote.txt', mkdir_pass], check=True)
                                            with open(mkdir_pass + '/result.txt', 'r', encoding='utf-8') as file:
                                                node_data = [list(map(float, line.strip().split())) for line in file]
                                                result_data.append(node_data)
                                        # 计算平均值
                                        average_data = []
                                        for row in zip(*result_data):
                                            avg_row = [sum(col) / len(col) for col in zip(*row)]
                                            average_data.append(avg_row)
                                        average_data[1] = [x * ComputeNodeNum for x in average_data[1]]

                                        result_pass0 =  figure_path + '/' + RunWorkloadTpye[workload_type] + "_" + mode + "_12_" + str(coro_num) + "_" + str(hot_num) + "_" + str(epoch_time) + "_" + str(read_only_ratio) + "_" + str(local_txn_ratio) + "_" + str(ComputeNodeNum) + "_" + str(delay_time) + "_" + str(wrong_num) + "_" + str(K) + "_" + str(H) + "_" + str(long_txn_rate) + "/"
                                        subprocess.run(['cp', './build/remote_server/remote_server.txt', result_pass0])

                                        result_pass =  figure_path + '/' + RunWorkloadTpye[workload_type] + "_" + mode + "_12_" + str(coro_num) + "_" + str(hot_num) + "_" + str(epoch_time) + "_" + str(read_only_ratio) + "_" + str(local_txn_ratio) + "_" + str(ComputeNodeNum) + "_" + str(delay_time) + "_" + str(wrong_num) + "_" + str(K) + "_" + str(H) + "_" + str(long_txn_rate)
                                        with open(result_pass + '/result.txt', 'w', encoding='utf-8') as result_file:
                                            result_file.write(f"Time taken by function: {average_data[0][0]}s\n")
                                            result_file.write(f"Throughtput: {average_data[1][0]}\n")
                                            result_file.write(f"Fetch remote ratio: {average_data[2][0]}\n")
                                            result_file.write(f"Lock ratio: {average_data[3][0]}\n")
                                            result_file.write(f"P50 Latency: {average_data[4][0]}us\n")
                                            result_file.write(f"P99 Latency: {average_data[5][0]}us\n")
                                            if RunWorkloadTpye[workload_type] == "smallbank":
                                                for i in range(len(SmallBank_TX_NAME)):
                                                    result_file.write(f"abort:{SmallBank_TX_NAME[i]} {average_data[6 + i][0]} {average_data[6 + i][1]}\n")
                                                result_file.write(f"tx_begin_time: {average_data[6 + len(SmallBank_TX_NAME)][0]}\n")
                                                result_file.write(f"tx_exe_time: {average_data[7 + len(SmallBank_TX_NAME)][0]}\n")
                                                result_file.write(f"tx_commit_time: {average_data[8 + len(SmallBank_TX_NAME)][0]}\n")
                                                result_file.write(f"tx_abort_time: {average_data[9 + len(SmallBank_TX_NAME)][0]}\n")
                                                result_file.write(f"tx_update_time: {average_data[10 + len(SmallBank_TX_NAME)][0]}\n")
                                                result_file.write(f"tx_fetch_exe_time: {average_data[11 + len(SmallBank_TX_NAME)][0]}\n")
                                                result_file.write(f"tx_fetch_commit_time: {average_data[12 + len(SmallBank_TX_NAME)][0]}\n")
                                                result_file.write(f"tx_fetch_abort_time: {average_data[13 + len(SmallBank_TX_NAME)][0]}\n")
                                                result_file.write(f"tx_release_exe_time: {average_data[14 + len(SmallBank_TX_NAME)][0]}\n")
                                                result_file.write(f"tx_release_commit_time: {average_data[15 + len(SmallBank_TX_NAME)][0]}\n")
                                                result_file.write(f"tx_release_abort_time: {average_data[16 + len(SmallBank_TX_NAME)][0]}\n")
                                                result_file.write(f"tx_get_timestamp_time1: {average_data[17 + len(SmallBank_TX_NAME)][0]}\n")
                                                result_file.write(f"tx_get_timestamp_time2: {average_data[18 + len(SmallBank_TX_NAME)][0]}\n")
                                                result_file.write(f"tx_write_commit_log_time: {average_data[19 + len(SmallBank_TX_NAME)][0]}\n")
                                                result_file.write(f"tx_write_prepare_log_time: {average_data[20 + len(SmallBank_TX_NAME)][0]}\n")
                                                result_file.write(f"tx_write_backup_log_time: {average_data[21 + len(SmallBank_TX_NAME)][0]}\n")
                                            elif RunWorkloadTpye[workload_type] == "tpcc":
                                                for i in range(len(TPCC_TX_NAME)):
                                                    result_file.write(f"abort:{TPCC_TX_NAME[i]} {average_data[6 + i][0]} {average_data[6 + i][1]}\n")
                                                result_file.write(f"tx_begin_time: {average_data[6 + len(TPCC_TX_NAME)][0]}\n")
                                                result_file.write(f"tx_exe_time: {average_data[7 + len(TPCC_TX_NAME)][0]}\n")
                                                result_file.write(f"tx_commit_time: {average_data[8 + len(TPCC_TX_NAME)][0]}\n")
                                                result_file.write(f"tx_abort_time: {average_data[9 + len(TPCC_TX_NAME)][0]}\n")
                                                result_file.write(f"tx_update_time: {average_data[10 + len(TPCC_TX_NAME)][0]}\n")
                                                result_file.write(f"tx_fetch_exe_time: {average_data[11 + len(TPCC_TX_NAME)][0]}\n")
                                                result_file.write(f"tx_fetch_commit_time: {average_data[12 + len(TPCC_TX_NAME)][0]}\n")
                                                result_file.write(f"tx_fetch_abort_time: {average_data[13 + len(TPCC_TX_NAME)][0]}\n")
                                                result_file.write(f"tx_release_exe_time: {average_data[14 + len(TPCC_TX_NAME)][0]}\n")
                                                result_file.write(f"tx_release_commit_time: {average_data[15 + len(TPCC_TX_NAME)][0]}\n")
                                                result_file.write(f"tx_release_abort_time: {average_data[16 + len(TPCC_TX_NAME)][0]}\n")
                                                result_file.write(f"tx_get_timestamp_time1: {average_data[17 + len(TPCC_TX_NAME)][0]}\n")
                                                result_file.write(f"tx_get_timestamp_time2: {average_data[18 + len(TPCC_TX_NAME)][0]}\n")
                                                result_file.write(f"tx_write_commit_log_time: {average_data[19 + len(TPCC_TX_NAME)][0]}\n")
                                                result_file.write(f"tx_write_prepare_log_time: {average_data[20 + len(TPCC_TX_NAME)][0]}\n")
                                                result_file.write(f"tx_write_backup_log_time: {average_data[21 + len(TPCC_TX_NAME)][0]}\n")
                                        #获取结果 TODO 在这个读取完结果后删除node1
                                        subprocess.run(["docker", "exec", "-it", "node1", "/bin/bash", "-c", "ps -ef | grep remote_node | grep -v grep | awk '{print $2}' | xargs kill -9"])
                                        subprocess.run(["docker", "exec", "-it", "node1", "/bin/bash", "-c", "ps -ef | grep storage_pool | grep -v grep | awk '{print $2}' | xargs kill -9"])
                                        print ("has write result to " + result_pass)
                                        time.sleep(5)
