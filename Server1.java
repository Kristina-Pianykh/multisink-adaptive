package server;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.*;
import java.net.*;
import utils.ControlMessage;

public class Server1 {
  // public static void main(String[] args) {
  //   int port = 6668;
  //
  //   try (ServerSocket serverSocket = new ServerSocket(port)) {
  //     System.out.println("Server started. Listening for connections on port " + port + "...");
  //     while (true) {
  //       Socket socket = serverSocket.accept();
  //       new ClientHandler(socket).start();
  //     }
  //   } catch (IOException e) {
  //     e.printStackTrace();
  //     System.exit(-1);
  //   }
  // }

  private static class ClientHandler extends Thread {
    private Socket socket;

    public ClientHandler(Socket socket) {
      this.socket = socket;
    }

    @Override
    public void run() {
      ObjectMapper mapper = new ObjectMapper();

      try (DataInputStream dis = new DataInputStream(socket.getInputStream())) {
        System.out.println(
            "Socket for the connection: "
                + socket.getInetAddress()
                + ":"
                + socket.getPort()
                + " is open.");
        while (true) {
          System.out.println("reading from input stream");
          try {
            ControlMessage message = mapper.readValue(dis.readUTF(), ControlMessage.class);
            // String message = dis.readUTF(); // Continuously read messages
            System.out.println("Client says: " + message.message);
          } catch (EOFException e) {
            System.out.println("Client has closed the connection.");
            break; // Exit the loop if EOFException is caught
          }
        }
      } catch (IOException e) {
        System.out.println("Client has closed the connection.");
        e.printStackTrace();
      } finally {
        try {
          socket.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }
}
