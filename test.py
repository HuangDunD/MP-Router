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
import argparse

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)


def kill_server():
    with open(output, "w", encoding="utf-8") as outfile:
        subprocess.run("ps -ef | grep run | grep -v grep | awk '{print $2}' | xargs kill -9",stdout=outfile, stderr=outfile,shell=True)
        subprocess.run("rm ./output.txt", stdout=outfile, stderr=outfile, shell=True)
    time.sleep(1)

def build():
    with open(output, "w", encoding="utf-8") as outfile:
        subprocess.run("rm -rf ./build", stdout=outfile, stderr=outfile, shell=True)
        subprocess.run("mkdir ./build", stdout=outfile, stderr=outfile, shell=True)
        subprocess.run("cd ./build && cmake ..", shell=True)
        subprocess.run("cd ./build && make -j8", shell=True)
    time.sleep(1)

def run_cmd(cmd, check=True):
    logging.info(f"Executing: {cmd}")
    result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8')
    if check and result.returncode != 0:
        logging.error(f"Command failed: {cmd}\nError: {result.stderr}")
        raise Exception(f"Command failed: {cmd}")
    return result

def run_remote_cmd(cmd, check=True, max_retries=3, allowed_exit_codes=[0]):
    logging.info(f"Executing Remote on {kwr_report_ip}: {cmd}")
    last_exception = None
    
    for attempt in range(1, max_retries + 1):
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            # 增加 timeout 防止连接卡死
            ssh.connect(kwr_report_ip, username="root", password="20001010@@HcY", timeout=30)
            # 开启 keepalive 防止长时间无数据传输导致连接断开 (特别是在 rsync 过程中)
            ssh.get_transport().set_keepalive(60)
            
            stdin, stdout, stderr = ssh.exec_command(cmd)
            exit_status = stdout.channel.recv_exit_status()
            out = stdout.read().decode('utf-8')
            err = stderr.read().decode('utf-8')
            
            if check and exit_status not in allowed_exit_codes:
                logging.error(f"Remote command failed: {cmd}\nExit Code: {exit_status}\nError: {err}")
                raise Exception(f"Remote command failed: {cmd}")
            
            # 如果是 rsync 返回 24，打印一个警告但视为成功
            if exit_status == 24:
                logging.warning(f"Rsync warning (code 24): Some files vanished during transfer. This is usually safe to ignore.")

            class Result:
                def __init__(self, stdout, stderr, returncode):
                    self.stdout = stdout
                    self.stderr = stderr
                    self.returncode = returncode
            
            return Result(out, err, exit_status)
        except Exception as e:
            logging.warning(f"Remote execution failed (Attempt {attempt}/{max_retries}): {e}")
            last_exception = e
            if attempt < max_retries:
                time.sleep(10) # 失败后等待一段时间再重试
        finally:
            ssh.close()
            
    logging.error(f"All {max_retries} attempts failed for command: {cmd}")
    raise last_exception

def get_remote_dir_size(path):
    # 获取目录大小 (KB)
    res = run_remote_cmd(f"du -s {path} | awk '{{print $1}}'", check=False)
    if res.returncode != 0:
        return -1
    try:
        return int(res.stdout.strip())
    except:
        return -1

def check_remote_exists(path):
    res = run_remote_cmd(f"test -e {path}", check=False)
    return res.returncode == 0

def wait_for_db_start(timeout=3000):
    logging.info("Waiting for database to start...")
    start_time = time.time()
    while time.time() - start_time < timeout:
        # 检查数据库资源状态
        res = run_remote_cmd("crm resource status clone-DB1", check=False)
        # 根据实际 crm 输出调整，通常 running 表示已启动
        if "is NOT running" not in res.stdout: 
            logging.info("Database resource appears to be started.")
            time.sleep(120) # 额外等待 120 秒确保数据库完全就绪可连接
            return
        time.sleep(15)
    raise Exception("Database failed to start within timeout")

def wait_for_db_stop(timeout=3000):
    logging.info("Waiting for database to stop...")
    start_time = time.time()
    while time.time() - start_time < timeout:
        # 检查数据库资源状态
        res = run_remote_cmd("crm resource status clone-DB1", check=False)
        # 如果不包含 "is running on"，则认为已停止
        if "is running on" not in res.stdout: 
            logging.info("Database resource appears to be stopped.")
            time.sleep(30) # 额外等待 30 秒确保数据库完全停止
            return
        time.sleep(15)
    raise Exception("Database failed to stop within timeout")

def reset_db_data(backup_path):
    logging.info(f">>> Resetting Database Data from {backup_path} <<<")
    
    # Check backup size first
    backup_size = get_remote_dir_size(backup_path)
    logging.info(f"Backup size: {backup_size} KB")
    
    # 修复：空目录大小通常为4KB，原先 <= 0 的判断会导致空目录被当作有效备份
    # 从而导致 rsync --delete 删空数据库。这里设置一个最小阈值，例如 100MB (102400 KB)
    min_backup_size = 100 * 1024 
    if backup_size < min_backup_size:
        error_msg = f"Critical Error: Backup size at {backup_path} is too small ({backup_size} KB). Refusing to restore to avoid data loss."
        logging.error(error_msg)
        raise Exception(error_msg)

    # 1. Stop Database
    run_remote_cmd("crm resource stop clone-DB1")
    wait_for_db_stop()
    time.sleep(10)
    
    # 2. Restore Data (使用 rsync 保证权限和完整性)
    # --delete 确保目标目录中多余的文件被删除，保持与源目录完全一致
    # -a 归档模式，保留权限、所有者等
    logging.info("Restoring data from backup...")
    
    # Check disk space before restore (optional debug)
    run_remote_cmd(f"df -h {database_data_path}", check=False)

    run_remote_cmd(f"rsync -a --delete --timeout=3000 {backup_path}/ {database_data_path}", allowed_exit_codes=[0, 24])
    
    # Verify restore
    restored_size = get_remote_dir_size(database_data_path)
    logging.info(f"Restored size: {restored_size} KB")
    
    if abs(restored_size - backup_size) > backup_size * 0.1:
        logging.error(f"CRITICAL: Restore size mismatch! Backup: {backup_size}, Restored: {restored_size}")
        # raise Exception("Restore failed: Size mismatch")

    # Fix permissions just in case (assuming kingbase user)
    run_remote_cmd(f"chown -R kingbase:kingbase {database_data_path}", check=False)
    run_remote_cmd("sync", check=False)

    # 3. Start Database
    run_remote_cmd("crm resource start clone-DB1")
    
    # 4. Wait for startup
    wait_for_db_start()
    logging.info(">>> Database Reset Complete <<<")

def prepare_backup_data(run_mode, access_pattern, account_count, worker_threads, extra_arg, backup_path):
    logging.info(f">>> Preparing Backup Data (Load & Backup) to {backup_path} <<<")
    
    # 1. 确保数据库是启动状态
    run_remote_cmd("crm resource start clone-DB1", check=False)
    wait_for_db_start()

    # 2. 运行导入数据 (使用 --load-data-only)
    # 使用 build 目录下的 run
    cmd = (
        f"./run --workload {workload} --load-data-only --system-mode {run_mode} --access-pattern {access_pattern}{extra_arg} "
        f"--account-count {account_count} --worker-threads {worker_threads} --sys_extend_size {sys_extend_size} --sys_index_extend_size {sys_index_extend_size}"
    )
    logging.info(f"Loading data with command: {cmd}")
    # 切换到运行目录执行
    cwd_backup = os.getcwd()
    os.chdir(Run_Path)
    run_cmd(cmd)
    os.chdir(cwd_backup)

    # 3. Stop Database
    logging.info("Stopping database for offline backup...")
    run_remote_cmd("crm resource stop clone-DB1")
    wait_for_db_stop()  # 确保数据库完全停止
    time.sleep(60)

    # 检查数据目录是否存在，如果不存在说明 stop 把磁盘也卸载了
    if not check_remote_exists(database_data_path):
        logging.error(f"CRITICAL: Data path '{database_data_path}' disappeared after stopping database!")
        logging.error("It seems 'crm resource stop' unmounted the storage.")
        logging.info("Attempting to restart database to recover mount point...")
        run_remote_cmd("crm resource start clone-DB1")
        wait_for_db_start()
        raise Exception(f"Cannot perform offline backup: Storage unmounted when DB stops.")

    # 4. Backup Data
    logging.info(f"Backing up data to {backup_path} ...")
    run_remote_cmd(f"mkdir -p {backup_path}")
    # 再次检查源目录防止 rsync 报错
    if check_remote_exists(database_data_path):
        run_remote_cmd(f"rsync -a --delete --timeout=3000 {database_data_path} {backup_path}/", allowed_exit_codes=[0, 24])
    else:
        raise Exception(f"Source path {database_data_path} not found")
    
    logging.info(">>> Backup Complete <<<")

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
database_data_path = "/sharedata/kingbase/data-hot/"

# !      !       !            注意：根据实际环境修改以上路径和参数                  !        !        !
# -------------------------------------------- # test parameters -------------------------------------------- #
# dynamic
# RunModeType = [0, 3, 8, 11, 4, 13]
# RunModeType = [0, 11, 13, 3, 4]
RunModeType = [11, 13]
AccessPattern = [2]
# AccessPattern = [0]
# ZipfianTheta = [0.95, 0.9, 0.8, 0.7]
ZipfianTheta = [0.8, 0.7]
HotspotFraction = [0.1, 0.01, 0.001]
HotspotProb = [0.8]
# account = 100W, 单个表大概14W个页面, 每个页面8KB, 大小约1.1GB
AccountCount = [5000000]
WorkerThreadCount = [16]
try_count = 10000
workload = "smallbank"
sys_extend_size = 300000
sys_index_extend_size = 50000
AffinityTxnRatio = [0.2]

# -------------------------------------------- # main test logic -------------------------------------------- #

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--force-reload", action="store_true", help="Force reload data even if backup exists")
    args = parser.parse_args()

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

    if AccountCount[0] < 2000000:
        sys_extend_size = 300000
    elif AccountCount[0] <= 5000000:
        sys_extend_size = 500000
    else:
        sys_extend_size = 800000

    if AccountCount[0] < 2000000:
        sys_index_extend_size = 10000
    elif AccountCount[0] <= 5000000:
        sys_index_extend_size = 50000
    else:
        sys_index_extend_size = 80000

    kill_server()
    build()
    
    # 标记是否已经准备好备份数据
    current_backup_key = None 

    for account_count in AccountCount:
        for access_pattern in AccessPattern:
            # 只有 access_pattern == 1 (zipfian) 才遍历 ZipfianTheta，其余模式不遍历
            if access_pattern == 1:
                param_list = ZipfianTheta
            elif access_pattern == 2:
                param_list = [(f, p) for f in HotspotFraction for p in HotspotProb]
            else:
                param_list = [None]
            
            for param in param_list:
                zipfian_theta = None
                hotspot_fraction = None
                hotspot_prob = None
                
                if access_pattern == 1:
                    zipfian_theta = param
                elif access_pattern == 2:
                    hotspot_fraction, hotspot_prob = param

                # 构造 extra_arg 用于 load data
                extra_arg_load = ""
                if access_pattern == 1:
                    extra_arg_load = f" --zipfian-theta {zipfian_theta}"
                elif access_pattern == 2:
                    extra_arg_load = f" --hotspot-fraction {hotspot_fraction} --hotspot-prob {hotspot_prob}"

                # 检查是否需要重新准备备份数据
                # 假设只有 account_count 变化才需要重新 load 数据
                backup_path = f"/sharedata/kingbase/{workload}_{account_count}"
                
                # 判断是否需要执行 prepare (Load & Backup)
                need_prepare = False
                
                if args.force_reload:
                    need_prepare = True
                elif not check_remote_exists(backup_path):
                    need_prepare = True
                
                # 避免在当前脚本运行期间重复 prepare 同一个 backup_path
                # current_backup_key 记录的是上一次成功 prepare 或 确认存在的 backup_path
                if backup_path != current_backup_key:
                    if need_prepare:
                        prepare_backup_data(RunModeType[0], access_pattern, account_count, WorkerThreadCount[0], extra_arg_load, backup_path)
                    else:
                        logging.info(f"Using existing backup at {backup_path}")
                    
                    current_backup_key = backup_path

                for affinity_ratio in AffinityTxnRatio:
                    for worker_thread_count in WorkerThreadCount:
                        for run_mode in RunModeType:
                            attempt = 0
                            success = False
                            
                            # 每次测试前重置数据
                            # reset_db_data(backup_path)
                            # 确保 server 进程被清理
                            kill_server() 

                            # 删除之前的结果文件，防止误判
                            if os.path.exists(result):
                                os.remove(result)

                            os.chdir(Run_Path)
                            while attempt < max_try and not success:
                                attempt += 1
                                extra_part_log = ""
                                if access_pattern == 1:
                                    extra_part_log = f", ZipfianTheta={zipfian_theta}"
                                elif access_pattern == 2:
                                    extra_part_log = f", HotspotFraction={hotspot_fraction}, HotspotProb={hotspot_prob}"

                                logging.info(
                                    f"Running test with RunMode={run_mode}, AccessPattern={access_pattern}{extra_part_log}, AccountCount={account_count}, WorkerThreads={worker_thread_count}, Attempt={attempt}"
                                )
                                kwr_report_name = f"kwr_mode{run_mode}_access{access_pattern}_acc{account_count}_thd{worker_thread_count}"

                                # 构造命令
                                extra_arg = ""
                                if access_pattern == 1:
                                    extra_arg = f" --zipfian-theta {zipfian_theta}"
                                elif access_pattern == 2:
                                    extra_arg = f" --hotspot-fraction {hotspot_fraction} --hotspot-prob {hotspot_prob}"
                                
                                # 注意：这里添加 --skip-load-data，因为我们已经通过 reset_db_data 恢复了数据
                                # cmd = (
                                #     f"./run --workload {workload} --system-mode {run_mode} --access-pattern {access_pattern}{extra_arg} "
                                #     f"--account-count {account_count} --worker-threads {worker_thread_count} --try-count {try_count} --kwr-name {kwr_report_name} --skip-load-data"
                                #     f" --sys_extend_size {sys_extend_size} --sys_index_extend_size {sys_index_extend_size} --affinity-txn-ratio {affinity_ratio}"
                                # )
                                cmd = (
                                    f"./run --workload {workload} --system-mode {run_mode} --access-pattern {access_pattern}{extra_arg} "
                                    f"--account-count {account_count} --worker-threads {worker_thread_count} --try-count {try_count} --kwr-name {kwr_report_name}"
                                    f" --sys_extend_size {sys_extend_size} --sys_index_extend_size {sys_index_extend_size} --affinity-txn-ratio {affinity_ratio}"
                                )
                                with open(output, "w", encoding="utf-8") as outfile:
                                    process = subprocess.Popen(cmd, shell=True)
                                    process.wait()
                                if os.path.exists(result):
                                    success = True
                                    logging.info("Test completed successfully.")

                                    # 创建结果文件夹
                                    extra_part_file = ""
                                    if access_pattern == 1:
                                        extra_part_file = f"_ZipfianTheta{zipfian_theta}"
                                    elif access_pattern == 2:
                                        extra_part_file = f"_HotspotFraction{hotspot_fraction}_HotspotProb{hotspot_prob}"

                                    dest_dir = (
                                        f"{figure_path}/result_RunMode{run_mode}_AccessPattern{access_pattern}{extra_part_file}_AccountCount{account_count}_WorkerThreads{worker_thread_count}_AffinityTxnRatio{affinity_ratio}/"
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
                                        # sftp.get(remote_warm_report, local_warm_report)
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
