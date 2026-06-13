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
    os.makedirs(os.path.dirname(output), exist_ok=True)
    with open(output, "w", encoding="utf-8") as outfile:
        subprocess.run("ps -ef | grep run | grep -v grep | awk '{print $2}' | xargs kill -9",stdout=outfile, stderr=outfile,shell=True)
        subprocess.run("rm ./output.txt", stdout=outfile, stderr=outfile, shell=True)
    time.sleep(1)

def build():
    os.makedirs(os.path.dirname(output), exist_ok=True)
    with open(output, "w", encoding="utf-8") as outfile:
        subprocess.run("rm -rf ./build", stdout=outfile, stderr=outfile, shell=True)
        subprocess.run("mkdir ./build", stdout=outfile, stderr=outfile, shell=True)
        subprocess.run("cd ./build && cmake ..", shell=True)
        subprocess.run("cd ./build && make -j8", shell=True)
    time.sleep(1)

original_config_h_text = None
current_built_mlp_mode = None

def set_mlp_prediction(enabled):
    global original_config_h_text
    config_path = os.path.join(workspace, "config.h")
    with open(config_path, "r", encoding="utf-8") as f:
        text = f.read()
    if original_config_h_text is None:
        original_config_h_text = text
    value = "1" if int(enabled) else "0"
    new_text, count = re.subn(
        r"^#define\s+MLP_PREDICTION\s+\d+(\s*//.*)?$",
        lambda m: f"#define MLP_PREDICTION {value}{m.group(1) or ''}",
        text,
        count=1,
        flags=re.MULTILINE,
    )
    if count != 1:
        raise RuntimeError("Unable to find MLP_PREDICTION definition in config.h")
    if new_text != text:
        logging.info(f"Setting MLP_PREDICTION={value} in config.h")
        with open(config_path, "w", encoding="utf-8") as f:
            f.write(new_text)

def restore_config_h():
    if original_config_h_text is None or not RestoreConfigAfterRun:
        return
    config_path = os.path.join(workspace, "config.h")
    with open(config_path, "w", encoding="utf-8") as f:
        f.write(original_config_h_text)
    logging.info("Restored original config.h")

def ensure_build_for_mlp(mlp_enabled):
    global current_built_mlp_mode
    if current_built_mlp_mode == int(mlp_enabled):
        return
    if not RebuildForMLP and current_built_mlp_mode is not None:
        raise RuntimeError("EnableMLP contains multiple values but RebuildForMLP is disabled.")
    kill_server()
    set_mlp_prediction(mlp_enabled)
    build()
    current_built_mlp_mode = int(mlp_enabled)

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

def workload_scale_value(workload_name, account_count=None, warehouse_count=None):
    if workload_name in ("tpcc", "tpcc-standard"):
        return warehouse_count
    return account_count

def workload_scale_arg(workload_name, account_count=None, warehouse_count=None):
    if workload_name in ("tpcc", "tpcc-standard"):
        return f"--warehouse-count {warehouse_count}"
    return f"--account-count {account_count}"

def workload_scale_label(workload_name, account_count=None, warehouse_count=None):
    if workload_name in ("tpcc", "tpcc-standard"):
        return f"wh{warehouse_count}"
    return f"acc{account_count}"

def data_cache_path(workload_name, account_count=None, warehouse_count=None):
    scale_value = workload_scale_value(workload_name, account_count, warehouse_count)
    if workload_name == "smallbank":
        return f"/sharedata/kingbase/{workload_name}_{scale_value}"
    return f"/sharedata/kingbase/{workload_name}_{workload_scale_label(workload_name, account_count, warehouse_count)}"

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

def ensure_data_cache(case, force_reload=False):
    backup_path = data_cache_path(case["workload"], case.get("account_count"), case.get("warehouse_count"))
    if force_reload or not data_cache_is_valid(backup_path):
        reason = "--force-reload" if force_reload else "no valid cache was detected"
        logging.info(f"Preparing data cache {backup_path}: {reason}")
        prepare_backup_data(case, backup_path)
    else:
        logging.info(f"Automatically using existing data cache: {backup_path}")
    return backup_path

def list_for_workload(mapping, workload_name, default_values):
    if isinstance(mapping, dict):
        return mapping.get(workload_name, default_values)
    return default_values

def access_patterns_for_workload(workload_name):
    return list_for_workload(WorkloadAccessPatterns, workload_name, AccessPattern)

def default_access_pattern_for_workload(workload_name):
    if isinstance(DefaultAccessPattern, dict):
        return DefaultAccessPattern.get(workload_name, access_patterns_for_workload(workload_name)[0])
    return DefaultAccessPattern

def default_scale_for_workload(workload_name):
    if workload_name in ("tpcc", "tpcc-standard"):
        return None, list_for_workload(WarehouseCount, workload_name, WarehouseCount)[0]
    return list_for_workload(AccountCount, workload_name, AccountCount)[0], None

def scale_values_for_workload(workload_name):
    if workload_name in ("tpcc", "tpcc-standard"):
        for warehouse_count in list_for_workload(WarehouseCount, workload_name, WarehouseCount):
            yield None, warehouse_count
    else:
        for account_count in list_for_workload(AccountCount, workload_name, AccountCount):
            yield account_count, None

def default_access_config(workload_name):
    access_pattern = default_access_pattern_for_workload(workload_name)
    if access_pattern == 1:
        return {
            "access_pattern": access_pattern,
            "zipfian_theta": DefaultZipfianTheta,
            "zipfian_generator": ZipfianGenerator,
            "hotspot_fraction": None,
            "hotspot_prob": None,
        }
    if access_pattern == 2:
        return {
            "access_pattern": access_pattern,
            "zipfian_theta": None,
            "zipfian_generator": None,
            "hotspot_fraction": DefaultHotspotFraction,
            "hotspot_prob": DefaultHotspotProb,
        }
    return {
        "access_pattern": access_pattern,
        "zipfian_theta": None,
        "zipfian_generator": None,
        "hotspot_fraction": None,
        "hotspot_prob": None,
    }

def access_configs_for_workload(workload_name):
    configs = []
    for access_pattern in access_patterns_for_workload(workload_name):
        if access_pattern == 1:
            for theta in ZipfianTheta:
                configs.append({
                    "access_pattern": access_pattern,
                    "zipfian_theta": theta,
                    "zipfian_generator": ZipfianGenerator,
                    "hotspot_fraction": None,
                    "hotspot_prob": None,
                })
        elif access_pattern == 2:
            for hotspot_fraction in HotspotFraction:
                for hotspot_prob in HotspotProb:
                    configs.append({
                        "access_pattern": access_pattern,
                        "zipfian_theta": None,
                        "zipfian_generator": None,
                        "hotspot_fraction": hotspot_fraction,
                        "hotspot_prob": hotspot_prob,
                    })
        else:
            configs.append({
                "access_pattern": access_pattern,
                "zipfian_theta": None,
                "zipfian_generator": None,
                "hotspot_fraction": None,
                "hotspot_prob": None,
            })
    return configs

def base_case_config(workload_name, account_count=None, warehouse_count=None):
    case = {
        "workload": workload_name,
        "account_count": account_count,
        "warehouse_count": warehouse_count,
        "worker_threads": DefaultWorkerThreads,
        "affinity_txn_ratio": DefaultAffinityTxnRatio,
        "batch_size": DefaultBatchSize,
        "num_bucket": DefaultNumBucket,
        "long_txn_length": DefaultLongTxnLength if EnableLongTxn else None,
        "key_page_ratio": DefaultKeyPageMapCapacity,
        "mlp_enabled": DefaultEnableMLP,
        "use_data_cache": UseDataCache,
        "scan_axis": "base",
    }
    case.update(default_access_config(workload_name))
    return case

def dedupe_case_configs(configs):
    deduped = []
    seen = set()
    for case in configs:
        key = (
            case["workload"],
            case.get("account_count"),
            case.get("warehouse_count"),
            case["access_pattern"],
            case.get("zipfian_theta"),
            case.get("zipfian_generator"),
            case.get("hotspot_fraction"),
            case.get("hotspot_prob"),
            case["worker_threads"],
            case["affinity_txn_ratio"],
            case["batch_size"],
            case["num_bucket"],
            case.get("long_txn_length"),
            case.get("key_page_ratio"),
            case.get("mlp_enabled"),
        )
        if key not in seen:
            seen.add(key)
            deduped.append(case)
    return deduped

def values_except_default(values, default_value):
    return [value for value in values if value != default_value]

def access_config_key(config):
    return (
        config["access_pattern"],
        config.get("zipfian_theta"),
        config.get("zipfian_generator"),
        config.get("hotspot_fraction"),
        config.get("hotspot_prob"),
    )

def build_axis_case_configs(workload_name, account_count=None, warehouse_count=None):
    base = base_case_config(workload_name, account_count, warehouse_count)
    configs = [base]

    default_access_key = access_config_key(default_access_config(workload_name))
    for access_config in access_configs_for_workload(workload_name):
        if access_config_key(access_config) == default_access_key:
            continue
        case = dict(base)
        case.update(access_config)
        case["scan_axis"] = "access"
        configs.append(case)

    if workload_name != "smallbank":
        return dedupe_case_configs(configs)

    for worker_threads in values_except_default(WorkerThreadCount, DefaultWorkerThreads):
        case = dict(base)
        case["worker_threads"] = worker_threads
        case["scan_axis"] = "worker_threads"
        configs.append(case)

    for affinity_ratio in values_except_default(AffinityTxnRatio, DefaultAffinityTxnRatio):
        case = dict(base)
        case["affinity_txn_ratio"] = affinity_ratio
        case["scan_axis"] = "affinity_txn_ratio"
        configs.append(case)

    for batch_size in values_except_default(BatchSize, DefaultBatchSize):
        case = dict(base)
        case["batch_size"] = batch_size
        case["scan_axis"] = "batch_size"
        configs.append(case)

    for num_bucket in values_except_default(NumBucket, DefaultNumBucket):
        case = dict(base)
        case["num_bucket"] = num_bucket
        case["scan_axis"] = "num_bucket"
        configs.append(case)

    if EnableLongTxn:
        for long_txn_length in values_except_default(LongTxnSize, DefaultLongTxnLength):
            case = dict(base)
            case["long_txn_length"] = long_txn_length
            case["scan_axis"] = "long_txn_length"
            configs.append(case)

    for key_page_ratio in values_except_default(KeyPageMapCapacity, DefaultKeyPageMapCapacity):
        case = dict(base)
        case["key_page_ratio"] = key_page_ratio
        case["scan_axis"] = "key_page_capacity"
        configs.append(case)

    for mlp_enabled in values_except_default(EnableMLP, DefaultEnableMLP):
        case = dict(base)
        case["mlp_enabled"] = mlp_enabled
        case["scan_axis"] = "mlp"
        configs.append(case)

    return dedupe_case_configs(configs)

def build_full_case_configs(workload_name, account_count=None, warehouse_count=None):
    configs = []
    long_txn_sizes = LongTxnSize if EnableLongTxn else [None]
    for access_config in access_configs_for_workload(workload_name):
        for worker_threads in WorkerThreadCount:
            for batch_size in BatchSize:
                for key_page_ratio in KeyPageMapCapacity:
                    for mlp_enabled in EnableMLP:
                        for affinity_ratio in AffinityTxnRatio:
                            for num_bucket in NumBucket:
                                for long_txn_length in long_txn_sizes:
                                    case = {
                                        "workload": workload_name,
                                        "account_count": account_count,
                                        "warehouse_count": warehouse_count,
                                        "worker_threads": worker_threads,
                                        "affinity_txn_ratio": affinity_ratio,
                                        "batch_size": batch_size,
                                        "num_bucket": num_bucket,
                                        "long_txn_length": long_txn_length,
                                        "key_page_ratio": key_page_ratio,
                                        "mlp_enabled": mlp_enabled,
                                        "use_data_cache": UseDataCache,
                                        "scan_axis": "full",
                                    }
                                    case.update(access_config)
                                    configs.append(case)
    return dedupe_case_configs(configs)

def build_case_configs_for_workload(workload_name, account_count=None, warehouse_count=None):
    if SweepMode == "axis":
        return build_axis_case_configs(workload_name, account_count, warehouse_count)
    if SweepMode == "full":
        return build_full_case_configs(workload_name, account_count, warehouse_count)
    raise ValueError(f"Unknown SweepMode: {SweepMode}")

def build_case_plan():
    main_case_pairs = []
    mlp_case_pairs = []
    seen = set()
    baseline_mlp = int(DefaultEnableMLP)
    mlp_run_modes = MLPRunModeType if MLPRunModeType else RunModeType
    key_page_capacity_run_modes = KeyPageCapacityRunModeType if KeyPageCapacityRunModeType else RunModeType

    def case_key(case_config, run_mode):
        return (
            case_config["workload"],
            case_config.get("account_count"),
            case_config.get("warehouse_count"),
            case_config["access_pattern"],
            case_config.get("zipfian_theta"),
            case_config.get("zipfian_generator"),
            case_config.get("hotspot_fraction"),
            case_config.get("hotspot_prob"),
            case_config["worker_threads"],
            case_config["affinity_txn_ratio"],
            case_config["batch_size"],
            case_config["num_bucket"],
            case_config.get("long_txn_length"),
            case_config.get("key_page_ratio"),
            case_config.get("mlp_enabled"),
            run_mode,
        )

    case_id = 0
    for workload_name in Workloads:
        for account_count, warehouse_count in scale_values_for_workload(workload_name):
            for case_config in build_case_configs_for_workload(workload_name, account_count, warehouse_count):
                is_mlp_delta = (
                    case_config.get("scan_axis") == "mlp"
                    and int(case_config.get("mlp_enabled", baseline_mlp)) != baseline_mlp
                )
                if case_config.get("scan_axis") == "key_page_capacity":
                    run_modes = key_page_capacity_run_modes
                elif is_mlp_delta:
                    run_modes = mlp_run_modes
                else:
                    run_modes = RunModeType
                for run_mode in run_modes:
                    key = case_key(case_config, run_mode)
                    if key in seen:
                        continue
                    seen.add(key)
                    case = dict(case_config)
                    case["run_mode"] = run_mode
                    case["data_cache_path"] = data_cache_path(workload_name, account_count, warehouse_count)
                    if is_mlp_delta:
                        mlp_case_pairs.append(case)
                    else:
                        main_case_pairs.append(case)

    cases = main_case_pairs + mlp_case_pairs
    for case in cases:
        case_id += 1
        case["case_id"] = case_id
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
        workload_scale_label(case["workload"], case.get("account_count"), case.get("warehouse_count")),
        f"t{case['worker_threads']}",
        f"r{value_for_path(case['affinity_txn_ratio'])}",
        f"b{case['batch_size']}",
        f"nb{case['num_bucket']}",
        f"kp{value_for_path(case['key_page_ratio'])}",
        f"mlp{case['mlp_enabled']}",
    ])
    if case.get("long_txn_length") is not None:
        parts.append(f"lt{case['long_txn_length']}")
    return "_".join(parts)

def summarize_result_dir(result_dir):
    summary_script = os.path.join(workspace, "scripts", "summarize_mp_router_results.py")
    subprocess.run([sys.executable, summary_script, result_dir], check=False)

def access_extra_args(case):
    if case["access_pattern"] == 1:
        return f" --zipfian-theta {case['zipfian_theta']} --zipfian-generator {case['zipfian_generator']}"
    if case["access_pattern"] == 2:
        return f" --hotspot-fraction {case['hotspot_fraction']} --hotspot-prob {case['hotspot_prob']}"
    return ""

def extend_sizes_for_case(case):
    if case["workload"] not in ("tpcc", "tpcc-standard") and case.get("account_count") is not None:
        account_count = case["account_count"]
        if account_count <= 2000000:
            return 100000, 10000
        if account_count <= 5000000:
            return 200000, 30000
        return 800000, 80000
    return sys_extend_size, sys_index_extend_size

def prepare_backup_data(case, backup_path):
    logging.info(f">>> Preparing Backup Data (Load & Backup) to {backup_path} <<<")
    
    # 1. 确保数据库是启动状态
    run_remote_cmd(f"crm resource start {db_resource_name}", check=False)
    wait_for_db_start()

    # 2. 运行导入数据 (使用 --load-data-only)
    # 使用 build 目录下的 run
    sys_extend, sys_index_extend = extend_sizes_for_case(case)
    cmd = (
        f"./run --workload {case['workload']} --load-data-only --system-mode {case['run_mode']} "
        f"--access-pattern {case['access_pattern']}{access_extra_args(case)} "
        f"{workload_scale_arg(case['workload'], case.get('account_count'), case.get('warehouse_count'))} "
        f"--worker-threads {case['worker_threads']} "
        f"--sys_extend_size {sys_extend} --sys_index_extend_size {sys_index_extend}"
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
enable_sql_readiness_probe = True
data_path_wait_timeout_seconds = 30
test_interval_seconds = 5
db_ready_probe_conninfos = [
    "host=172.16.0.105 port=44321 user=system password=123456 dbname=smallbank",
    "host=172.16.0.113 port=44321 user=system password=123456 dbname=smallbank",
    "host=172.16.0.114 port=44321 user=system password=123456 dbname=smallbank",
    "host=172.16.0.115 port=44321 user=system password=123456 dbname=smallbank",
]


# !      !       !            注意：根据实际环境修改以上路径和参数                  !        !        !
# -------------------------------------------- # test parameters -------------------------------------------- #
# dynamic
# RunModeType = [0, 3, 8, 11, 4, 13]
# RunModeType = [0, 3, 11, 13]
RunModeType = [0, 2, 11, 13, 23, 25, 28]
# RunModeType = [11, 13, 2]
# RunModeType = [28]
# ! system: 0 随机路由, 2 page hash 11 MP-Router 13 MP-Router without scheduling 23 metis 24 ownership + load 25 load
# RunModeType = [13]
# RunModeType = [1]
Workloads = ["smallbank", "tpcc"] # one script run can cover multiple workloads
WorkloadAccessPatterns = {
    "smallbank": [1, 2],
    "tpcc": [0],
}
SweepMode = "axis" # axis: vary one dimension from defaults; full: Cartesian product
AccessPattern = [1, 2, 0] # 0 uniform, 1 zipfian, 2 hotspot
# AccessPattern = [1]
# ZipfianTheta = [0.4]
# ZipfianTheta = [0.8]
ZipfianTheta = [0.1, 0.3, 0.7, 0.9, 1.1, 1.3] 
# ZipfianTheta = [0.8, 0.9, 0.95, 0.7, 0.6]
ZipfianGenerator = "finite" # options: finite, legacy
HotspotFraction = [1, 0.1, 0.01, 0.001]
HotspotProb = [0.8]
# HotspotProb = [0.8, 0.9, 0.95]
# account = 100W, 单个表大概14W个页面, 每个页面8KB, 大小约1.1GB
AccountCount = [10000000]
WarehouseCount = [200]
# WorkerThreadCount = [16]
WorkerThreadCount = [16, 2, 4, 8, 32, 64, 128]
try_count = 35000
TimeRun = 1 # 0:disable, 1:enable
WarmupSeconds = 15
RunSeconds = 30
FillPipelineBubble = 0
Unlog = 1
UseDataCache = False # True: restore workload data cache before each case; False: do not restart DB, load data in each run
workload = Workloads[0]
sys_extend_size = 300000
sys_index_extend_size = 30000
AffinityTxnRatio = [0.8, 1, 0.6, 0.4, 0.2, 0]
# AffinityTxnRatio = [1, 0.8, 0.6, 0.4, 0.2, 0]
BatchSize = [10000] # default 10000
# BatchSize = [1000]
NumBucket = [4]
EnableLongTxn = 0 # 0:disable, 1:enable
LongTxnSize = [4, 8, 12, 14, 16, 20] # only valid when EnableLongTxn=1
KeyPageMapCapacity = [1.1, 1.0, 0.8, 0.6, 0.4, 0.2] # passed to --key-page-ratio
EnableMLP = [0] # 0:disable, 1:enable; changing this requires rebuilding with MLP_PREDICTION
MLPRunModeType = [11] # MLP-delta cases run only these modes; baseline MLP=0 reuses normal sweep results
KeyPageCapacityRunModeType = [1, 23, 11, 28] # KeyPageMapCapacity axis runs only these modes
RebuildForMLP = True
RestoreConfigAfterRun = True

DefaultAccessPattern = {
    "smallbank": 1,
    "tpcc": 0,
}
DefaultZipfianTheta = 0.7
DefaultHotspotFraction = 0.01
DefaultHotspotProb = 0.8
DefaultWorkerThreads = WorkerThreadCount[0]
DefaultBatchSize = BatchSize[0]
DefaultKeyPageMapCapacity = KeyPageMapCapacity[0]
DefaultEnableMLP = EnableMLP[0]
DefaultAffinityTxnRatio = AffinityTxnRatio[0]
DefaultNumBucket = NumBucket[0]
DefaultLongTxnLength = LongTxnSize[0]

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
    if args.plan_only:
        logging.info(f"Plan-only mode completed: {figure_path}")
        raise SystemExit(0)

    # !开始本次的测试
    os.chdir(workspace)
    atexit.register(restore_config_h)
    
    # 标记是否已经准备好备份数据
    current_backup_key = None 

    for case in planned_cases:
        ensure_build_for_mlp(case["mlp_enabled"])
        backup_path = case["data_cache_path"]
        if UseDataCache and backup_path != current_backup_key:
            backup_path = ensure_data_cache(case, force_reload=args.force_reload)
            current_backup_key = backup_path

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
            if case["access_pattern"] == 1:
                extra_part_log = f", ZipfianTheta={case['zipfian_theta']}, ZipfianGenerator={case['zipfian_generator']}"
            elif case["access_pattern"] == 2:
                extra_part_log = f", HotspotFraction={case['hotspot_fraction']}, HotspotProb={case['hotspot_prob']}"

            scale_label = workload_scale_label(case["workload"], case.get("account_count"), case.get("warehouse_count"))
            logging.info(
                f"Running case {case['case_id']} with Workload={case['workload']}, RunMode={case['run_mode']}, "
                f"AccessPattern={case['access_pattern']}{extra_part_log}, Scale={scale_label}, "
                f"WorkerThreads={case['worker_threads']}, BatchSize={case['batch_size']}, "
                f"KeyPageCapacity={case['key_page_ratio']}, MLP={case['mlp_enabled']}, Attempt={attempt}"
            )
            kwr_report_name = (
                f"kwr_{time_str}_case{case['case_id']:03d}"
                f"_mode{case['run_mode']}_access{case['access_pattern']}"
                f"_{case['workload']}_{scale_label}_thd{case['worker_threads']}"
            )

            sys_extend, sys_index_extend = extend_sizes_for_case(case)
            cmd = (
                f"./run --workload {case['workload']} --system-mode {case['run_mode']} "
                f"--access-pattern {case['access_pattern']}{access_extra_args(case)} "
                f"{workload_scale_arg(case['workload'], case.get('account_count'), case.get('warehouse_count'))} "
                f"--worker-threads {case['worker_threads']} --kwr-name {kwr_report_name}"
                f" --sys_extend_size {sys_extend} --sys_index_extend_size {sys_index_extend} "
                f"--affinity-txn-ratio {case['affinity_txn_ratio']} "
                f" --batch-size {case['batch_size']} --num-bucket {case['num_bucket']}"
                f" --key-page-ratio {case['key_page_ratio']}"
            )
            if UseDataCache:
                cmd += " --skip-load-data"

            if TimeRun:
                cmd += f" --time-run --warmup-seconds {WarmupSeconds} --run-seconds {RunSeconds} --fill-pipeline-bubble {FillPipelineBubble}"
                if Unlog:
                    cmd += " --unlog"
            else:
                current_try_count = try_count
                if EnableLongTxn and case.get("long_txn_length"):
                    current_try_count = 35000 // case["long_txn_length"]
                cmd += f" --try-count {current_try_count}"

            if EnableLongTxn and case.get("long_txn_length"):
                cmd += f" --enable-long-txn --long-txn-length {case['long_txn_length']}"

            wait_for_db_start()

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
                    f"m{case['run_mode']}",
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
                remote_run_report = os.path.join(kwr_report_path, f"{kwr_report_name}_end.html")
                local_run_report = os.path.join(dest_dir, f"{kwr_report_name}_end.html")
                try:
                    sftp.get(remote_run_report, local_run_report)
                except Exception as e:
                    logging.error(f"\033[31m Failed to retrieve KWR report files: {e} \033[0m")
                sftp.close()
                ssh.close()
                summarize_result_dir(figure_path)
            else:
                logging.warning("Result file not found, retrying...")
        if not success:
            logging.error(
                f"Test failed after {max_try} attempts for case {case['case_id']}: "
                f"Workload={case['workload']}, RunMode={case['run_mode']}, AccessPattern={case['access_pattern']}, "
                f"Scale={workload_scale_label(case['workload'], case.get('account_count'), case.get('warehouse_count'))}"
            )

        time.sleep(test_interval_seconds)
    kill_server()
    summarize_result_dir(figure_path)
    logging.info("All tests completed.")
