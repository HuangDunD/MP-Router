package mock.bench.Tpcc.Tool;

import java.io.*;
import java.net.Socket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static mock.bench.Tpcc.WorkLoad.jTPCC.*;

public class ExecClient {

    private Socket socket;
    private BufferedReader reader;
    private  DataOutputStream writer;
    private Connection[] connections;
    private final java.util.Random random = new java.util.Random(31); // 使用固定种子
    private boolean offline = false;
    private boolean jdbc = false;
    private static final String startMarker = "***Txn_Start***\n";
    private static final String endMarker = "***Txn_End***\n";

    public ExecClient(String host, int port){
        try {
            switch (loadType) {
                case "online":
                    offline = false;
                    connect(host, port);
                    break;
                case "offline":
                    offline = true;
                    File file = new File(offlineLoadFilePath);
                    writer = new DataOutputStream(new FileOutputStream(file));
                    break;
                case "jdbc":
                    jdbc = true;
                    connections = new Connection[iConn.length];
                    for (int i = 0; i < iConn.length; i++) {
                        connections[i] = DriverManager.getConnection(iConn[i], iUser[i], iPassword[i]);
                    }
                    break;
                default:
                    System.err.println("Unknown load type: " + loadType);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to connect to host: " + e.getMessage());
        }
    }

    public void connect(String host, int port) throws IOException {
        // 建立 TCP 连接
        socket = new Socket(host, port);
        socket.setReceiveBufferSize(1024 * 1024); // 1 MB
        socket.setSendBufferSize(1024 * 1024); // 1 MB

        // 使用 BufferedReader 和 BufferedWriter 处理输入输出流
        reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        writer = new DataOutputStream(socket.getOutputStream());
        sendTxn("HELLO");
    }

    public void sendTxn(String txn) throws IOException {
        if (jdbc){
            executeSql(txn);
        } else {
            int totalLength = txn.length() + 1;
            writer.writeInt(totalLength); // 发送消息长度
            writer.write((txn + "\n").getBytes()); // 发送消息
            writer.flush();    // 确保数据被发送

            if (!offline){
                String response = reader.readLine();
                if (response.contains("ERR"))
                    System.out.println("Success: " + response);
            }
        }
    }

    public void executeSql(String sqlBuilderContent) {
        try {
            int startIndex = sqlBuilderContent.indexOf(startMarker) + startMarker.length();
            int endIndex = sqlBuilderContent.indexOf(endMarker);

            if (startIndex < 0 || endIndex < 0 || startIndex >= endIndex) {
                throw new IllegalArgumentException("Invalid SQL block markers.");
            }

            String sqlBlock = sqlBuilderContent.substring(startIndex, endIndex).trim();
            
            // 随机选择连接
            Connection currentConn = connections[random.nextInt(connections.length)];

            try (Statement statement = currentConn.createStatement()) {
                String[] sqlStatements = sqlBlock.split(";\n");
                for (String sql : sqlStatements) {
                    sql = sql.trim();
                    statement.execute(sql);
                }
            } catch (Exception e) {
                throw new SQLException("Failed to execute SQL block: " + e.getMessage(), e);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to execute SQL: " + e.getMessage());
        }
    }

    public void close() throws IOException {
        // 关闭资源
        if (jdbc){
            for (Connection conn : connections) {
                try {
                    if (conn != null) {
                        conn.close();
                    }
                } catch (Exception ignored) {}
            }
        } else {
            if (!offline){
                writer.write("Bye\n".getBytes());
                writer.flush();      // 确保数据被发送
                writer.close();
                reader.close();
                socket.close();
            } else {
                writer.flush();      // 确保数据被发送
                writer.close();
            }
        }
    }
}
