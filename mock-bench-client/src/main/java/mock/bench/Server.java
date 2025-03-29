package mock.bench;

import java.io.*;
import java.net.*;

public class Server { // 仅本地测试使用
    public static void main(String[] args) {
        int port = 8500;

        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("Server is listening on port " + port);

            while (true) {
                Socket socket = serverSocket.accept();
                System.out.println("New client connected");

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
            try (
                    InputStream input = clientSocket.getInputStream();
                    BufferedReader reader = new BufferedReader(new InputStreamReader(input))
            ) {
                String text;

                // Read messages from the client
                while ((text = reader.readLine()) != null) {
                    System.out.println("Received message from client: " + text);
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



