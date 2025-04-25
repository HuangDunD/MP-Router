package mock.bench.TpchYcsb;

import org.apache.tools.ant.taskdefs.Sleep;

import java.io.*;
import java.net.*;

import static java.lang.System.exit;

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
                // Handle the client in a new thread
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
            try {
                InputStream input = clientSocket.getInputStream();
                BufferedReader reader = new BufferedReader(new InputStreamReader(input));
                OutputStream output = clientSocket.getOutputStream();
                BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(output));
                String text;

                // Read messages from the client
                String txn = "", header = "";
                boolean collect_txn = true, collect_header = false;
                while ((text = reader.readLine()) != null) {
//                    System.out.println("Received message from client: " + text);
                    if (text.contains("BYE")) {
                        break;
                    } else if (text.contains("HELLO")) {
                        writer.write("FINISH");
                        writer.newLine(); // Add a newline character after each message
                        writer.flush();   // Ensure data is sent immediately
                    }
                    else if(text.contains("***Header_Start***")) {
                        collect_header = true;
                    } else if(text.contains("***Header_End***")) {
                        collect_txn = false;
                    } else if(text.contains("***Txn_Start***")) {
                        collect_txn = true;
                        collect_header = false;
                    } else if(text.contains("***Txn_End***")) {
                        collect_txn = false;
                        collect_header = false;
                        System.out.println("Txn_Header: [" + header + "]");
                        System.out.println("Txn_Body: [" + txn + "]");
                        txn = "";
                        header = "";
                        writer.write("FINISH");
                        writer.newLine(); // Add a newline character after each message
                        writer.flush();   // Ensure data is sent immediately
                    } else if(collect_header) {
                        header = header + text + "\n";
                    } else if (collect_txn) {
                        txn = txn + text + "\n";
                    }
                }
            } catch (IOException ex) {
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



