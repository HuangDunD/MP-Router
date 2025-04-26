package mock.bench.TpchYcsb;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;


public class Client {
    private static final String SERVER_ADDRESS = "127.0.0.1"; // 替换为服务器地址
    private static final int SERVER_PORT = 8500; // 替换为服务器端口
    private static Socket socket;
    private static PrintWriter out;

    public static void client_init(){
        try {
            // 初始化连接到服务器
            socket = new Socket(SERVER_ADDRESS, SERVER_PORT);
            out = new PrintWriter(socket.getOutputStream(), true);
            System.out.println("Connected to server at " + SERVER_ADDRESS + ":" + SERVER_PORT);
        } catch (IOException e) {
            System.err.println("Error connecting to server: " + e.getMessage());
            closeResources();
        }
    }

    // 发送消息
    public static synchronized void sendMessage(String message) {
        try {
//            System.out.println("Sending message: " + "***" + message + "###");
            // out.println("***" + message + "###");
            out.println(message);
        } catch (Exception e) {
            System.err.println("Error sending message: " + e.getMessage());
            closeResources();
        }
    }

    // 关闭资源
    public static void closeResources() {
        try {
            if (out != null) {
                out.close();
            }
            if (socket != null && !socket.isClosed()) {
                socket.close();
            }
        } catch (IOException e) {
            System.err.println("Error closing resources: " + e.getMessage());
        }
    }
}
