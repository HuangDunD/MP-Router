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
import shutil

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)


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
        subprocess.run("cd ./build && cmake ..", shell=True)
        subprocess.run("cd ./build && make -j8", shell=True)
    time.sleep(1)


# run settings
# const
max_try = 5
workspace = os.getcwd()
output = workspace + "/build/output.txt"
result = workspace + "/build/serve/test/result.txt"
log = workspace + "/build/serve/test/partitioning_log.log"
Run_Path = workspace + "/build/serve/test/"
kwr_report_ip = "10.10.2.41"
kwr_report_path = "/home/kingbase/MP-Router/kwr/"

# dynamic
# RunModeType = [0, 3, 8, 11, 4]
RunModeType = [11, 3, 8]
AccessPattern = [0]
# AccessPattern = [0]
ZipfianTheta = [0.3, 0.6, 0.75, 0.9]
AccountCount = [300000]
WorkerThreadCount = [32]
try_count = 50000

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

    kill_server()
    build()
    for account_count in AccountCount:
        for access_pattern in AccessPattern:
            # 只有 access_pattern == 1 (zipfian) 才遍历 ZipfianTheta，其余模式不遍历
            theta_list = ZipfianTheta if access_pattern == 1 else [None]
            for zipfian_theta in theta_list:
                for worker_thread_count in WorkerThreadCount:
                    for run_mode in RunModeType:
                        attempt = 0
                        success = False
                        os.chdir(Run_Path)
                        while attempt < max_try and not success:
                            attempt += 1
                            theta_part_log = f", ZipfianTheta={zipfian_theta}" if access_pattern == 1 else ""
                            logging.info(
                                f"Running test with RunMode={run_mode}, AccessPattern={access_pattern}{theta_part_log}, AccountCount={account_count}, WorkerThreads={worker_thread_count}, Attempt={attempt}"
                            )
                            kwr_report_name = f"kwr_mode{run_mode}_access{access_pattern}_acc{account_count}_thd{worker_thread_count}"

                            # 构造命令：只有 zipfian 时才附带 --zipfian-theta 参数
                            theta_arg = f" --zipfian-theta {zipfian_theta}" if access_pattern == 1 else ""
                            cmd = (
                                f"./smallbank_test --system-mode {run_mode} --access-pattern {access_pattern}{theta_arg} "
                                f"--account-count {account_count} --worker-threads {worker_thread_count} --try-count {try_count} --kwr-name {kwr_report_name}"
                            )
                            with open(output, "w", encoding="utf-8") as outfile:
                                process = subprocess.Popen(cmd, shell=True)
                                process.wait()
                            if os.path.exists(result):
                                success = True
                                logging.info("Test completed successfully.")

                                # 创建结果文件夹
                                theta_part_file = (
                                    f"_ZipfianTheta{zipfian_theta}" if access_pattern == 1 else ""
                                )
                                dest_dir = (
                                    f"{figure_path}/result_RunMode{run_mode}_AccessPattern{access_pattern}{theta_part_file}_AccountCount{account_count}_WorkerThreads{worker_thread_count}/"
                                )
                                os.makedirs(dest_dir, exist_ok=True)

                                # 使用shutil复制文件，保持内容一致
                                dest_file = f"{dest_dir}result.txt"
                                shutil.copy2(result, dest_file)
                                log_file = f"{dest_dir}partitioning_log.log"
                                shutil.copy2(log, log_file)

                                # scp 将远程服务器的 kwr 报告文件复制到本地对应的结果文件夹中
                                ssh = paramiko.SSHClient()
                                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                                ssh.connect(kwr_report_ip, username="root", password="20001010@@HcY")
                                sftp = ssh.open_sftp()
                                remote_warm_report = os.path.join(kwr_report_path, f"{kwr_report_name}_fisrt.html")
                                remote_run_report = os.path.join(kwr_report_path, f"{kwr_report_name}_end.html")
                                local_warm_report = os.path.join(dest_dir, f"{kwr_report_name}_fisrt.html")
                                local_run_report = os.path.join(dest_dir, f"{kwr_report_name}_end.html")
                                try:
                                    sftp.get(remote_warm_report, local_warm_report)
                                    sftp.get(remote_run_report, local_run_report)
                                except Exception as e:
                                    logging.error(f"\033[31m Failed to retrieve KWR report files: {e} \033[0m")
                                sftp.close()
                                ssh.close()
                                
                            else:
                                logging.warning("Result file not found, retrying...")
                        if not success:
                            theta_err = f", ZipfianTheta={zipfian_theta}" if access_pattern == 1 else ""
                            logging.error(
                                f"Test failed after {max_try} attempts for RunMode={run_mode}, AccessPattern={access_pattern}{theta_err}, AccountCount={account_count}, WorkerThreads={worker_thread_count}"
                            )
    kill_server()
    logging.info("All tests completed.")
