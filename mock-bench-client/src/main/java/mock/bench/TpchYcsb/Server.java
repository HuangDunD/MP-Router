package mock.bench.TpchYcsb;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

public class Server { // 仅本地测试使用
    public static void main(String[] args) {
        int port = 8500;
        int count = 1;

        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("Server is listening on port " + port);

            while (true) {
                Socket socket = serverSocket.accept();
                System.out.println("New client connected: " + count);
                count++;
                new Thread(new ClientHandler(socket)).start();
            }
        } catch (IOException ex) {
            System.err.println("Server exception: " + ex.getMessage());
            ex.printStackTrace();
        }
    }

    private static class ClientHandler implements Runnable {
        private final Socket clientSocket;

        public ClientHandler(Socket socket) {
            this.clientSocket = socket;
        }

        @Override
        public void run() {
            try (
                    InputStream input = clientSocket.getInputStream();
                    DataInputStream dis = new DataInputStream(input);
                    OutputStream output = clientSocket.getOutputStream();
                    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(output))
            ) {
                String txn = "", header = "";
                boolean collect_txn = true, collect_header = false;

                while (true) {
                    // 1. 读取前4个字节作为 int，表示后续文本长度（单位：字节）
                    int totalBytes = dis.readInt();

                    // 2. 读取指定字节数
                    byte[] buffer = new byte[totalBytes];
                    dis.readFully(buffer); // 安全读取所有字节

                    // 3. 转换为字符串（假设客户端用 UTF-8 编码发送）
                    String blockText = new String(buffer, StandardCharsets.UTF_8);
                    // 以\n为分隔符分割字符串
                    List<String> txnList = Arrays.asList(blockText.split("\n"));

//                    System.out.println("Received block with total bytes: " + totalBytes);
//                    System.out.println("Block content:\n" + blockText);

                    // 4. 使用 BufferedReader 逐行处理
//                    BufferedReader lineReader = new BufferedReader(new StringReader(blockText));
//                    String text;
//                    while ((text = lineReader.readLine()) != null) {
//                        System.out.println("Received message from client: " + text);
                    for (String text : txnList) {
                        if (text.contains("BYE")) {
                            clientSocket.close();
                            return; // 结束连接
                        } else if (text.contains("HELLO")) {
                            writer.write("FINISH");
                            writer.newLine();
                            writer.flush();
                        } else if (text.contains("***Header_Start***")) {
                            collect_header = true;
                        } else if (text.contains("***Header_End***")) {
                            collect_txn = false;
                        } else if (text.contains("***Txn_Start***")) {
                            collect_txn = true;
                            collect_header = false;
                        } else if (text.contains("***Txn_End***")) {
                            collect_txn = false;
                            collect_header = false;
                            writer.write("FINISH");
                            writer.newLine();
                            writer.flush();
                            Thread.sleep(1); // Sleep for 1 second
                            txn = "";
                            header = "";
                        } else if (collect_header) {
                            header += text + "\n";
                        } else if (collect_txn) {
                            txn += text + "\n";
                        }
                    }
                }
            } catch (IOException | InterruptedException ex) {
                System.err.println("Client handler error: " + ex.getMessage());
            } finally {
                try {
                    clientSocket.close();
                } catch (IOException ex) {
                    System.err.println("Error closing client socket: " + ex.getMessage());
                }
            }
        }
    }
}