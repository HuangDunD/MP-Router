import socket
import time

# 服务器地址和端口
HOST = '127.0.0.1'  # 本地主机，可以改为服务器的实际IP
PORT = 8500         # 与服务器监听的端口一致

def send_test_data():
    # 创建TCP socket
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        # 连接到服务器
        client_socket.connect((HOST, PORT))
        print(f"已连接到 {HOST}:{PORT}")

        # 测试数据列表
        test_messages = [
            """UPDATE benchbase.usertable
            SET FIELD1 = CASE
        WHEN YCSB_KEY = 10 THEN SUBSTRING(MD5(RAND()), 1, 10)
        WHEN YCSB_KEY = 1001 THEN SUBSTRING(MD5(RAND()), 1, 10)
        WHEN YCSB_KEY = 2012 THEN SUBSTRING(MD5(RAND()), 1, 10)
        WHEN YCSB_KEY = 3013 THEN SUBSTRING(MD5(RAND()), 1, 10)
        WHEN YCSB_KEY = 4014 THEN SUBSTRING(MD5(RAND()), 1, 10)
        WHEN YCSB_KEY = 5015 THEN SUBSTRING(MD5(RAND()), 1, 10)
        WHEN YCSB_KEY = 6016 THEN SUBSTRING(MD5(RAND()), 1, 10)
        WHEN YCSB_KEY = 7017 THEN SUBSTRING(MD5(RAND()), 1, 10)
        WHEN YCSB_KEY = 8018 THEN SUBSTRING(MD5(RAND()), 1, 10)
        WHEN YCSB_KEY = 10019 THEN SUBSTRING(MD5(RAND()), 1, 10)
        ELSE FIELD1
        END
        WHERE YCSB_KEY IN (10, 1001, 2012, 3013, 4014, 5015, 6016, 7017, 8018, 10019);
        """
        ]

        # 发送测试数据
        for message in test_messages:
            # 发送消息
            client_socket.send(message.encode('utf-8'))
            print(f"发送: {message}")

            # 接收服务器响应
            response = client_socket.recv(1024).decode('utf-8')
            print(f"收到响应: {response}")

            # 短暂等待，避免消息发送过快
            time.sleep(1)

    except ConnectionRefusedError:
        print(f"连接失败，请确保服务器在 {HOST}:{PORT} 上运行")
    except Exception as e:
        print(f"发生错误: {e}")
    finally:
        # 关闭socket
        client_socket.close()
        print("连接已关闭")

if __name__ == "__main__":
    send_test_data()