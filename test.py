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
import csv
import shlex
import re

# Set LD_LIBRARY_PATH for YashanDB client
lib_path = os.path.expanduser("~/yashandb-client/lib")
if "LD_LIBRARY_PATH" in os.environ:
    os.environ["LD_LIBRARY_PATH"] = lib_path + ":" + os.environ["LD_LIBRARY_PATH"]
else:
    os.environ["LD_LIBRARY_PATH"] = lib_path

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

def run_remote_cmd(cmd, check=True, max_retries=3, allowed_exit_codes=[0], display_cmd=None):
    logging.info(f"Executing Remote on {kwr_report_ip}: {display_cmd or cmd}")
    last_exception = None
    
    for attempt in range(1, max_retries + 1):
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            # 增加 timeout 防止连接卡死
            ssh.connect(kwr_report_ip, username="root", password=kwr_ip_password, timeout=30)
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
                time.sleep(remote_retry_sleep_seconds)
        finally:
            ssh.close()
            
    logging.error(f"All {max_retries} attempts failed for command: {cmd}")
    raise last_exception

def run_remote_cmd_streaming(cmd, check=True, allowed_exit_codes=(0, 24)):
    logging.info(f"Executing Remote with live progress on {kwr_report_ip}: {cmd}")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        ssh.connect(kwr_report_ip, username="root", password=kwr_ip_password, timeout=30)
        ssh.get_transport().set_keepalive(60)
        _, stdout, stderr = ssh.exec_command(cmd, get_pty=True)
        while not stdout.channel.exit_status_ready():
            if stdout.channel.recv_ready():
                text = stdout.channel.recv(4096).decode("utf-8", errors="replace")
                print(text, end="", flush=True)
            if stdout.channel.recv_stderr_ready():
                text = stdout.channel.recv_stderr(4096).decode("utf-8", errors="replace")
                print(text, end="", file=sys.stderr, flush=True)
            time.sleep(0.05)
        while stdout.channel.recv_ready():
            print(stdout.channel.recv(4096).decode("utf-8", errors="replace"), end="", flush=True)
        while stdout.channel.recv_stderr_ready():
            print(stdout.channel.recv_stderr(4096).decode("utf-8", errors="replace"), end="", file=sys.stderr, flush=True)
        exit_status = stdout.channel.recv_exit_status()
        if check and exit_status not in allowed_exit_codes:
            raise Exception(f"Remote command failed with exit code {exit_status}: {cmd}")
        return exit_status
    finally:
        ssh.close()

def get_remote_dir_size(path):
    # 获取目录大小 (KB)
    res = run_remote_cmd(f"du -s {shlex.quote(path)} | awk '{{print $1}}'", check=False)
    if res.returncode != 0:
        return -1
    try:
        return int(res.stdout.strip())
    except:
        return -1

def check_remote_exists(path):
    res = run_remote_cmd(f"test -e {shlex.quote(path)}", check=False)
    return res.returncode == 0

def wait_for_remote_path(path, timeout=30, interval=2):
    logging.info(f"Waiting for remote path to be available: {path}")
    start_time = time.time()
    while time.time() - start_time < timeout:
        if check_remote_exists(path):
            return True
        time.sleep(interval)
    return False

def parse_conninfo(conninfo):
    parsed = {}
    for item in shlex.split(conninfo):
        if "=" not in item:
            continue
        key, value = item.split("=", 1)
        parsed[key] = value
    return parsed

def build_db_probe_command(conninfo):
    parsed = parse_conninfo(conninfo)
    host = parsed.get("host", "127.0.0.1")
    port = parsed.get("port", "5432")
    user = parsed.get("user", "system")
    password = parsed.get("password", "")
    dbname = parsed.get("dbname", parsed.get("database", workload))
    return (
        "if command -v ksql >/dev/null 2>&1; then client=ksql; "
        "elif command -v psql >/dev/null 2>&1; then client=psql; "
        "else exit 127; fi; "
        f"PGPASSWORD={shlex.quote(password)} timeout 5 \"$client\" "
        f"-h {shlex.quote(host)} -p {shlex.quote(str(port))} "
        f"-U {shlex.quote(user)} -d {shlex.quote(dbname)} "
        "-Atqc 'select 1' >/dev/null 2>&1"
    )

def build_local_pg_isready_command(conninfo):
    parsed = parse_conninfo(conninfo)
    host = parsed.get("host", "127.0.0.1")
    port = parsed.get("port", "5432")
    user = parsed.get("user", "system")
    dbname = parsed.get("dbname", parsed.get("database", workload))
    return [
        "pg_isready",
        "-h", host,
        "-p", str(port),
        "-U", user,
        "-d", dbname,
        "-t", str(db_pg_isready_timeout_seconds),
    ]

def build_local_sql_probe_command(conninfo):
    parsed = parse_conninfo(conninfo)
    host = parsed.get("host", "127.0.0.1")
    port = parsed.get("port", "5432")
    user = parsed.get("user", "system")
    dbname = parsed.get("dbname", parsed.get("database", workload))
    return [
        "psql",
        "-h", host,
        "-p", str(port),
        "-U", user,
        "-d", dbname,
        "-Atqc", "select 1",
    ]

def build_tcp_probe_command(conninfo):
    parsed = parse_conninfo(conninfo)
    host = parsed.get("host", "127.0.0.1")
    port = parsed.get("port", "5432")
    return (
        f"timeout {db_tcp_probe_timeout_seconds} "
        f"bash -lc '</dev/tcp/{shlex.quote(host)}/{shlex.quote(str(port))}'"
    )

def mask_conninfo_password(conninfo):
    return re.sub(r"password=[^ ]+", "password=***", conninfo)

def local_database_probe_ready(conninfo):
    parsed = parse_conninfo(conninfo)
    password = parsed.get("password", "")
    env = os.environ.copy()
    if password:
        env["PGPASSWORD"] = password

    if shutil.which("pg_isready"):
        cmd = build_local_pg_isready_command(conninfo)
        logging.info(f"Local pg_isready probe: {mask_conninfo_password(conninfo)}")
        res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env)
        if res.returncode != 0:
            logging.info(f"pg_isready not ready yet: rc={res.returncode}, stdout={res.stdout.strip()}, stderr={res.stderr.strip()}")
            return False, True

    if shutil.which("psql"):
        cmd = build_local_sql_probe_command(conninfo)
        logging.info(f"Local SQL readiness probe: {mask_conninfo_password(conninfo)}")
        res = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=env,
            timeout=db_sql_probe_timeout_seconds,
        )
        if res.returncode != 0:
            logging.info(f"SQL probe not ready yet: rc={res.returncode}, stdout={res.stdout.strip()}, stderr={res.stderr.strip()}")
            return False, True
        return True, True

    return False, False

def probe_database_ready():
    if not db_ready_probe_conninfos:
        logging.warning("No db_ready_probe_conninfos configured; using CRM status only.")
        return True, True

    if use_local_db_readiness_probe:
        for conninfo in db_ready_probe_conninfos:
            ready, probe_available = local_database_probe_ready(conninfo)
            if not probe_available:
                logging.warning("Neither pg_isready nor psql was found locally; falling back to remote TCP readiness probe.")
                break
            if not ready:
                return False, True
        else:
            return True, True

    for conninfo in db_ready_probe_conninfos:
        cmd = build_tcp_probe_command(conninfo)
        res = run_remote_cmd(
            cmd,
            check=False,
            max_retries=1,
            display_cmd=f"db tcp probe: {mask_conninfo_password(conninfo)}",
        )
        if res.returncode != 0:
            return False, True

    if not enable_sql_readiness_probe:
        return True, True

    probe_available = True
    for conninfo in db_ready_probe_conninfos:
        cmd = build_db_probe_command(conninfo)
        res = run_remote_cmd(
            cmd,
            check=False,
            max_retries=1,
            display_cmd=f"db readiness probe: {mask_conninfo_password(conninfo)}",
        )
        if res.returncode == 127:
            probe_available = False
            break
        if res.returncode != 0:
            return False, True
    return True, probe_available

def wait_for_db_start(timeout=3000):
    logging.info("Waiting for database to start...")
    start_time = time.time()
    while time.time() - start_time < timeout:
        # 检查数据库资源状态
        res = run_remote_cmd(f"crm resource status {db_resource_name}", check=False)
        # 根据实际 crm 输出调整，通常 running 表示已启动
        status_text = res.stdout + res.stderr
        if res.returncode == 0 and "is NOT running" not in status_text and "not found" not in status_text.lower():
            ready, probe_available = probe_database_ready()
            if ready:
                logging.info("Database connection probe succeeded.")
                return
            if not probe_available:
                logging.warning("Neither ksql nor psql was found on remote host; falling back to a short grace wait after CRM start.")
                time.sleep(db_start_probe_fallback_sleep_seconds)
                return
            logging.info("Database resource is running, but SQL connection is not ready yet.")
        time.sleep(db_status_poll_seconds)
    raise Exception("Database failed to start within timeout")

def wait_for_db_stop(timeout=3000):
    logging.info("Waiting for database to stop...")
    start_time = time.time()
    while time.time() - start_time < timeout:
        # 检查数据库资源状态
        res = run_remote_cmd(f"crm resource status {db_resource_name}", check=False)
        # 如果不包含 "is running on"，则认为已停止
        status_text = res.stdout + res.stderr
        if "is running on" not in status_text and "Started:" not in status_text:
            logging.info("Database resource appears to be stopped.")
            return
        time.sleep(db_status_poll_seconds)
    raise Exception("Database failed to stop within timeout")

def reset_db_data(backup_path):
    logging.info(f">>> Resetting Database Data from {backup_path} <<<")
    if backup_path.rstrip("/") == database_data_path.rstrip("/"):
        raise Exception("Data source directory must differ from the active database data directory.")
    
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
    run_remote_cmd(f"crm resource stop {db_resource_name}")
    wait_for_db_stop()
    
    # 2. Restore Data (使用 rsync 保证权限和完整性)
    # --delete 确保目标目录中多余的文件被删除，保持与源目录完全一致
    # -a 归档模式，保留权限、所有者等
    logging.info("Restoring data from backup...")
    
    # Check disk space before restore (optional debug)
    run_remote_cmd(f"df -h {shlex.quote(database_data_path)}", check=False)

    source = shlex.quote(backup_path.rstrip("/") + "/")
    destination = shlex.quote(database_data_path)
    run_remote_cmd_streaming(
        f"rsync -a --delete --info=progress2 --human-readable --timeout=3000 {source} {destination}"
    )
    
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
    run_remote_cmd(f"crm resource start {db_resource_name}")
    
    # 4. Wait for startup
    wait_for_db_start()
    logging.info(">>> Database Reset Complete <<<")

def data_cache_path(workload_name, account_count):
    return f"/sharedata/kingbase/{workload_name}_{account_count}"

def data_cache_is_valid(backup_path):
    if not check_remote_exists(backup_path):
        return False
    backup_size = get_remote_dir_size(backup_path)
    min_backup_size = 100 * 1024
    if backup_size < min_backup_size:
        logging.warning(
            f"Ignoring invalid data cache {backup_path}: size is only {backup_size} KB"
        )
        return False
    return True

def ensure_data_cache(run_mode, access_pattern, account_count, worker_threads, extra_arg, force_reload=False):
    backup_path = data_cache_path(workload, account_count)
    if force_reload or not data_cache_is_valid(backup_path):
        reason = "--force-reload" if force_reload else "no valid cache was detected"
        logging.info(f"Preparing data cache {backup_path}: {reason}")
        prepare_backup_data(
            run_mode, access_pattern, account_count, worker_threads, extra_arg, backup_path
        )
    else:
        logging.info(f"Automatically using existing data cache: {backup_path}")
    return backup_path

def build_case_plan():
    cases = []
    case_id = 0
    for account_count in AccountCount:
        for access_pattern in AccessPattern:
            if access_pattern == 1:
                param_list = ZipfianTheta
            elif access_pattern == 2:
                param_list = [(f, p) for f in HotspotFraction for p in HotspotProb]
            else:
                param_list = [None]

            for param in param_list:
                zipfian_theta = param if access_pattern == 1 else None
                zipfian_generator = ZipfianGenerator if access_pattern == 1 else None
                hotspot_fraction, hotspot_prob = param if access_pattern == 2 else (None, None)
                for num_bucket in NumBucket:
                    for affinity_ratio in AffinityTxnRatio:
                        for worker_thread_count in WorkerThreadCount:
                            for batch_size in BatchSize:
                                txn_sizes = LongTxnSize if EnableLongTxn else [None]
                                key_page_ratios = KeyPageRatio if EnableKeyPageRatio else [None]
                                for long_txn_length in txn_sizes:
                                    for key_page_ratio in key_page_ratios:
                                        for run_mode in RunModeType:
                                            case_id += 1
                                            cases.append({
                                                "case_id": case_id,
                                                "run_mode": run_mode,
                                                "workload": workload,
                                                "access_pattern": access_pattern,
                                                "zipfian_theta": zipfian_theta,
                                                "zipfian_generator": zipfian_generator,
                                                "hotspot_fraction": hotspot_fraction,
                                                "hotspot_prob": hotspot_prob,
                                                "account_count": account_count,
                                                "worker_threads": worker_thread_count,
                                                "affinity_txn_ratio": affinity_ratio,
                                                "batch_size": batch_size,
                                                "num_bucket": num_bucket,
                                                "long_txn_length": long_txn_length,
                                                "key_page_ratio": key_page_ratio,
                                                "use_data_cache": UseDataCache,
                                                "data_cache_path": data_cache_path(workload, account_count),
                                            })
    return cases

def write_case_plan(cases, plan_path):
    columns = list(cases[0].keys()) if cases else []
    with open(plan_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns, delimiter="\t")
        writer.writeheader()
        writer.writerows(cases)
    markdown_path = os.path.splitext(plan_path)[0] + ".md"
    with open(markdown_path, "w", encoding="utf-8") as f:
        f.write(f"# MP-Router Case Plan\n\nTotal cases: {len(cases)}\n\n")
        if columns:
            f.write("| " + " | ".join(columns) + " |\n")
            f.write("| " + " | ".join(["---"] * len(columns)) + " |\n")
            for case in cases:
                f.write("| " + " | ".join(str(case.get(column, "")) for column in columns) + " |\n")
    logging.info(f"Case plan written: {plan_path} ({len(cases)} cases)")

def write_case_metadata(case, dest_dir, kwr_report_name, command):
    metadata = dict(case)
    metadata["kwr_report_name"] = kwr_report_name
    metadata["command"] = command
    with open(os.path.join(dest_dir, "metadata.json"), "w", encoding="utf-8") as f:
        json.dump(metadata, f, ensure_ascii=False, indent=2)

def value_for_path(value):
    return str(value).replace("/", "_")

def case_group_dir_name(case):
    parts = [
        case["workload"],
        f"p{case['access_pattern']}",
    ]
    if case.get("zipfian_theta") is not None:
        parts.append(f"ZipfTheta{value_for_path(case['zipfian_theta'])}")
    if case.get("zipfian_generator") is not None:
        parts.append(f"ZipfGen{value_for_path(case['zipfian_generator'])}")
    if case.get("hotspot_fraction") is not None:
        parts.append(f"HsFrac{value_for_path(case['hotspot_fraction'])}")
    if case.get("hotspot_prob") is not None:
        parts.append(f"HsProb{value_for_path(case['hotspot_prob'])}")
    parts.extend([
        f"c{case['account_count']}",
        f"t{case['worker_threads']}",
        f"r{value_for_path(case['affinity_txn_ratio'])}",
        f"b{case['batch_size']}",
        f"nb{case['num_bucket']}",
    ])
    if case.get("long_txn_length") is not None:
        parts.append(f"lt{case['long_txn_length']}")
    if case.get("key_page_ratio") is not None:
        parts.append(f"kp{value_for_path(case['key_page_ratio'])}")
    return "_".join(parts)

def summarize_result_dir(result_dir):
    summary_script = os.path.join(workspace, "scripts", "summarize_mp_router_results.py")
    subprocess.run([sys.executable, summary_script, result_dir], check=False)

def prepare_backup_data(run_mode, access_pattern, account_count, worker_threads, extra_arg, backup_path):
    logging.info(f">>> Preparing Backup Data (Load & Backup) to {backup_path} <<<")
    
    # 1. 确保数据库是启动状态
    run_remote_cmd(f"crm resource start {db_resource_name}", check=False)
    wait_for_db_start()

    # 2. 运行导入数据 (使用 --load-data-only)
    # 使用 build 目录下的 run
    cmd = (
        f"./run --workload {workload} --load-data-only --system-mode {run_mode} --access-pattern {access_pattern}{extra_arg} "
        f"--account-count {account_count} --worker-threads {worker_threads} --sys_extend_size {sys_extend_size} --sys_index_extend_size {sys_index_extend_size}"
    )
    if Unlog:
        cmd += " --unlog"
    logging.info(f"Loading data with command: {cmd}")
    # 切换到运行目录执行
    cwd_backup = os.getcwd()
    os.chdir(Run_Path)
    run_cmd(cmd)
    os.chdir(cwd_backup)

    # 3. Stop Database
    logging.info("Stopping database for offline backup...")
    run_remote_cmd(f"crm resource stop {db_resource_name}")
    wait_for_db_stop()  # 确保数据库完全停止

    # 检查数据目录是否存在，如果不存在说明 stop 把磁盘也卸载了
    if not wait_for_remote_path(database_data_path, timeout=data_path_wait_timeout_seconds):
        logging.error(f"CRITICAL: Data path '{database_data_path}' disappeared after stopping database!")
        logging.error("It seems 'crm resource stop' unmounted the storage.")
        logging.info("Attempting to restart database to recover mount point...")
        run_remote_cmd(f"crm resource start {db_resource_name}")
        wait_for_db_start()
        raise Exception(f"Cannot perform offline backup: Storage unmounted when DB stops.")

    # 4. Backup Data
    logging.info(f"Backing up data to {backup_path} ...")
    run_remote_cmd(f"mkdir -p {backup_path}")
    # 再次检查源目录防止 rsync 报错
    if check_remote_exists(database_data_path):
        source = shlex.quote(database_data_path.rstrip("/") + "/")
        destination = shlex.quote(backup_path.rstrip("/") + "/")
        run_remote_cmd_streaming(
            f"rsync -a --delete --info=progress2 --human-readable --timeout=3000 {source} {destination}"
        )
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
kwr_report_ip = "47.111.27.99"
kwr_ip_password = "Wljwlj123."
kwr_report_path = "/home/kingbase/MP-Router/kwr/"
database_data_path = "/sharedata/kingbase/data-hot/"
db_resource_name = "clone-DB"
remote_retry_sleep_seconds = 3
db_status_poll_seconds = 5
db_start_probe_fallback_sleep_seconds = 5
db_tcp_probe_timeout_seconds = 2
db_pg_isready_timeout_seconds = 2
db_sql_probe_timeout_seconds = 5
use_local_db_readiness_probe = True
enable_sql_readiness_probe = False
data_path_wait_timeout_seconds = 30
test_interval_seconds = 1
db_ready_probe_conninfos = [
    "host=172.16.0.105 port=44321 user=system password=123456 dbname=smallbank",
    "host=172.16.0.109 port=44321 user=system password=123456 dbname=smallbank",
    "host=172.16.0.111 port=44321 user=system password=123456 dbname=smallbank",
    "host=172.16.0.110 port=44321 user=system password=123456 dbname=smallbank",
]


# !      !       !            注意：根据实际环境修改以上路径和参数                  !        !        !
# -------------------------------------------- # test parameters -------------------------------------------- #
# dynamic
# RunModeType = [0, 3, 8, 11, 4, 13]
# RunModeType = [0, 3, 11, 13]
RunModeType = [0, 2, 11, 13, 23, 25]
# RunModeType = [11, 13, 2]
# RunModeType = [28]
# ! system: 0 随机路由, 2 page hash 11 MP-Router 13 MP-Router without scheduling 23 metis 24 ownership + load 25 load
# RunModeType = [13]
# RunModeType = [1]
AccessPattern = [1, 2, 0] # 0 uniform, 1 zipfian, 2 hotspot
# AccessPattern = [1]
# ZipfianTheta = [0.4]
# ZipfianTheta = [0.8]
ZipfianTheta = [0.1, 0.3, 0.7, 0.9, 1.1, 1.3] 
# ZipfianTheta = [0.8, 0.9, 0.95, 0.7, 0.6]
ZipfianGenerator = "finite" # options: finite, legacy
HotspotFraction = [0.1, 0.01, 0.001]
HotspotProb = [0.8]
# HotspotProb = [0.8, 0.9, 0.95]
# account = 100W, 单个表大概14W个页面, 每个页面8KB, 大小约1.1GB
AccountCount = [5000000]
# WorkerThreadCount = [16]
WorkerThreadCount = [16]
try_count = 35000
TimeRun = 1 # 0:disable, 1:enable
WarmupSeconds = 10
RunSeconds = 30
FillPipelineBubble = 0
Unlog = 1
UseDataCache = False # True: restore workload data cache before each case; False: do not restart DB, load data in each run
workload = "smallbank"
sys_extend_size = 300000
sys_index_extend_size = 30000
AffinityTxnRatio = [0.8]
# AffinityTxnRatio = [1, 0.8, 0.6, 0.4, 0.2, 0]
BatchSize = [10000] # default 10000
# BatchSize = [1000]
NumBucket = [4]
EnableLongTxn = 0 # 0:disable, 1:enable
LongTxnSize = [4, 8, 12, 14, 16, 20] # only valid when EnableLongTxn=1
EnableKeyPageRatio = 0 # 0:disable, 1:enable
KeyPageRatio = [0.2, 0.4, 0.6, 0.8, 1.0] # only valid when EnableKeyPageRatio=1

# -------------------------------------------- # main test logic -------------------------------------------- #

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--force-reload", action="store_true", help="Force reload data even if backup exists")
    parser.add_argument(
        "--no-data-cache",
        action="store_true",
        help="Override UseDataCache and disable automatic workload data-cache detection, creation, and restore.",
    )
    parser.add_argument(
        "--plan-only",
        action="store_true",
        help="Generate case_plan.tsv and case_plan.md without building or running experiments.",
    )
    parser.add_argument("--enable-long-txn", action="store_true", help="Enable long transactions")
    parser.add_argument("--long-txn-length", type=int, default=None, help="Length of long transactions")
    parser.add_argument("--zipfian-generator", choices=("finite", "legacy"), default=None, help="Zipfian generator type")
    args = parser.parse_args()

    # Handle LongTxnSize override
    if args.long_txn_length is not None:
        LongTxnSize = [args.long_txn_length]
    if args.zipfian_generator is not None:
        ZipfianGenerator = args.zipfian_generator
    UseDataCache = UseDataCache and not args.no_data_cache
    logging.info(f"UseDataCache = {UseDataCache}")
    logging.info(f"ZipfianGenerator = {ZipfianGenerator}")

    # 删除之前的结果
    if os.path.exists(result):
        os.remove(result)
    # 创建图像文件夹
    if not os.path.exists("./result"):
        os.mkdir("./result")
    os.chdir("./result")
    # 创建此次测试的结果文件夹，以时间命名
    base_time_str = time.strftime("%Y%m%d%H%M%S", time.localtime())
    for suffix in range(1000):
        time_str = base_time_str if suffix == 0 else f"{base_time_str}_{suffix}"
        try:
            os.mkdir(time_str)
            break
        except FileExistsError:
            continue
    else:
        raise RuntimeError(f"Unable to create unique result directory for {base_time_str}")
    figure_path = os.path.join(os.getcwd(), time_str)
    planned_cases = build_case_plan()
    write_case_plan(planned_cases, os.path.join(figure_path, "case_plan.tsv"))
    planned_case_iter = iter(planned_cases)
    if args.plan_only:
        logging.info(f"Plan-only mode completed: {figure_path}")
        raise SystemExit(0)

    # !开始本次的测试
    os.chdir(workspace)

    if AccountCount[0] <= 2000000:
        sys_extend_size = 100000
    elif AccountCount[0] <= 5000000:
        sys_extend_size = 200000
    else:
        sys_extend_size = 800000

    if AccountCount[0] <= 2000000:
        sys_index_extend_size = 10000
    elif AccountCount[0] <= 5000000:
        sys_index_extend_size = 30000
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
                zipfian_generator = None
                hotspot_fraction = None
                hotspot_prob = None
                
                if access_pattern == 1:
                    zipfian_theta = param
                    zipfian_generator = ZipfianGenerator
                elif access_pattern == 2:
                    hotspot_fraction, hotspot_prob = param

                # 构造 extra_arg 用于 load data
                extra_arg_load = ""
                if access_pattern == 1:
                    extra_arg_load = f" --zipfian-theta {zipfian_theta} --zipfian-generator {zipfian_generator}"
                elif access_pattern == 2:
                    extra_arg_load = f" --hotspot-fraction {hotspot_fraction} --hotspot-prob {hotspot_prob}"

                backup_path = data_cache_path(workload, account_count)
                if UseDataCache and backup_path != current_backup_key:
                    backup_path = ensure_data_cache(
                        RunModeType[0],
                        access_pattern,
                        account_count,
                        WorkerThreadCount[0],
                        extra_arg_load,
                        force_reload=args.force_reload,
                    )
                    current_backup_key = backup_path

                for num_bucket in NumBucket:
                    for affinity_ratio in AffinityTxnRatio:
                        for worker_thread_count in WorkerThreadCount:
                            for batch_size in BatchSize:
                                current_txn_sizes = LongTxnSize if EnableLongTxn else [None]
                                current_key_page_ratios = KeyPageRatio if EnableKeyPageRatio else [None]
                                for long_txn_length in current_txn_sizes:
                                    for key_page_ratio in current_key_page_ratios:
                                        for run_mode in RunModeType:
                                            case = next(planned_case_iter)
                                            attempt = 0
                                            success = False

                                            if UseDataCache:
                                                reset_db_data(backup_path)
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
                                                    extra_part_log = f", ZipfianTheta={zipfian_theta}, ZipfianGenerator={zipfian_generator}"
                                                elif access_pattern == 2:
                                                    extra_part_log = f", HotspotFraction={hotspot_fraction}, HotspotProb={hotspot_prob}"

                                                logging.info(
                                                    f"Running test with RunMode={run_mode}, AccessPattern={access_pattern}{extra_part_log}, AccountCount={account_count}, WorkerThreads={worker_thread_count}, Attempt={attempt}"
                                                )
                                                kwr_report_name = (
                                                    f"kwr_{time_str}_case{case['case_id']:03d}"
                                                    f"_mode{run_mode}_access{access_pattern}"
                                                    f"_acc{account_count}_thd{worker_thread_count}"
                                                )

                                                # 构造命令
                                                extra_arg = ""
                                                if access_pattern == 1:
                                                    extra_arg = f" --zipfian-theta {zipfian_theta} --zipfian-generator {zipfian_generator}"
                                                elif access_pattern == 2:
                                                    extra_arg = f" --hotspot-fraction {hotspot_fraction} --hotspot-prob {hotspot_prob}"

                                                cmd = (
                                                    f"./run --workload {workload} --system-mode {run_mode} --access-pattern {access_pattern}{extra_arg} "
                                                    f"--account-count {account_count} --worker-threads {worker_thread_count} --kwr-name {kwr_report_name}"
                                                    f" --sys_extend_size {sys_extend_size} --sys_index_extend_size {sys_index_extend_size} --affinity-txn-ratio {affinity_ratio} "
                                                    f" --batch-size {batch_size} --num-bucket {num_bucket}"
                                                )
                                                if UseDataCache:
                                                    cmd += " --skip-load-data"

                                                if TimeRun:
                                                    cmd += f" --time-run --warmup-seconds {WarmupSeconds} --run-seconds {RunSeconds} --fill-pipeline-bubble {FillPipelineBubble}"
                                                    if Unlog:
                                                        cmd += " --unlog"
                                                else:
                                                    current_try_count = try_count
                                                    if EnableLongTxn:
                                                        current_try_count = 35000 // long_txn_length
                                                    cmd += f" --try-count {current_try_count}"

                                                if EnableLongTxn:
                                                    cmd += f" --enable-long-txn --long-txn-length {long_txn_length}"

                                                if EnableKeyPageRatio:
                                                    cmd += f" --key-page-ratio {key_page_ratio}"

                                                with open(output, "w", encoding="utf-8") as outfile:
                                                    process = subprocess.Popen(cmd, shell=True)
                                                    process.wait()
                                                if os.path.exists(result):
                                                    success = True
                                                    logging.info("Test completed successfully.")

                                                    # 同一组负载参数放在一个目录下，mode 作为下一层目录，避免顶层结果过散。
                                                    dest_dir = os.path.join(
                                                        figure_path,
                                                        case_group_dir_name(case),
                                                        f"m{run_mode}",
                                                    )
                                                    os.makedirs(dest_dir, exist_ok=True)
                                                    write_case_metadata(case, dest_dir, kwr_report_name, cmd)

                                                    # 使用shutil复制文件，保持内容一致
                                                    dest_file = os.path.join(dest_dir, "result.txt")
                                                    shutil.copy2(result, dest_file)
                                                    log_file = os.path.join(dest_dir, "partitioning_log.log")
                                                    shutil.copy2(log, log_file)

                                                    # scp 将远程服务器的 kwr 报告文件复制到本地对应的结果文件夹中
                                                    ssh = paramiko.SSHClient()
                                                    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                                                    ssh.connect(kwr_report_ip, username="root", password=kwr_ip_password)
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
                                                    summarize_result_dir(figure_path)
                                                else:
                                                    logging.warning("Result file not found, retrying...")
                                            if not success:
                                                theta_err = (
                                                    f", ZipfianTheta={zipfian_theta}, ZipfianGenerator={zipfian_generator}"
                                                    if access_pattern == 1 else ""
                                                )
                                                logging.error(
                                                    f"Test failed after {max_try} attempts for RunMode={run_mode}, AccessPattern={access_pattern}{theta_err}, AccountCount={account_count}, WorkerThreads={worker_thread_count}"
                                                )

                                            time.sleep(test_interval_seconds)
    kill_server()
    summarize_result_dir(figure_path)
    logging.info("All tests completed.")
