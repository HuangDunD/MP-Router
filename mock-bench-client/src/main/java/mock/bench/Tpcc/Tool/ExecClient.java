package mock.bench.Tpcc.Tool;

import java.io.*;
import java.net.Socket;

import static mock.bench.Tpcc.WorkLoad.jTPCC.offlineLoad;
import static mock.bench.Tpcc.WorkLoad.jTPCC.offlineLoadFilePath;

public class ExecClient {

    private Socket socket;
    private BufferedReader reader;
    private BufferedWriter writer;
    private boolean offline = false;

    public ExecClient(String host, int port){
        try {
            if (offlineLoad.equals("false")){
                offline = false;
                connect(host, port);
            }
            else {
                offline = true;
                File file = new File(offlineLoadFilePath);
                writer = new BufferedWriter(new FileWriter(file, true));
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to connect to host: " + e.getMessage());
        }
    }

    public void connect(String host, int port) throws IOException {
        // 建立 TCP 连接
        socket = new Socket(host, port);

        // 使用 BufferedReader 和 BufferedWriter 处理输入输出流
        reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));

        socket.setReceiveBufferSize(1024 * 1024); // 1 MB
        socket.setSendBufferSize(1024 * 1024); // 1 MB

        sendTxn("HELLO");
    }

    public void sendTxn(String txn) throws IOException {
        writer.write(txn); // 写入消息
        writer.newLine();
        writer.flush();    // 确保数据被发送

        if (!offline){
            String response = reader.readLine();
            while (response.equals("")){
                response = reader.readLine();
            }
        }
    }

    public void close() throws IOException {
        // 关闭资源
        if (!offline){
            writer.write("BYE"); // 发送关闭信号
            writer.newLine();    // 添加换行符
            writer.flush();      // 确保数据被发送
            writer.close();
            reader.close();
            socket.close();
            System.out.println("Connection closed.");
        } else {
            writer.flush();      // 确保数据被发送
            writer.close();
            System.out.println("File closed.");
        }
    }
}
