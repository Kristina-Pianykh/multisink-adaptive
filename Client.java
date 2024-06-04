// import com.fasterxml.jackson.core.JsonProcessingException;
// import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.*;
import java.net.*;
import java.util.concurrent.ThreadLocalRandom;

public class Client {

  // public static void main(String[] args) {
  //   // int[] ports = {6667, 6668};
  //   int[] ports = {6668};
  //
  //   for (int i = 0; i < ports.length; i++) {
  //     new PortSender(ports[i]).start();
  //   }
  // }

  public static class PortSender extends Thread {
    private int port;

    public PortSender(int port) {
      this.port = port;
    }

    @Override
    public void run() {
      try {
        Socket socket = new Socket("localhost", port);
        // DataOutputStream out = new DataOutputStream(socket.getOutputStream());
        for (int i = 0; i < 10; i++) {
          String message = String.format("{\"control\":\"true\",\"message\":\"%s\"}", i);

          Retry.send(socket, message);
          System.out.println("Sent message from loop iter " + i);
        }
        socket.close(); // Close the socket once all messages are sent
      } catch (ConnectException e) {
        System.out.println("Server is not running. Please start the server and try again.");
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  private static class Retry {
    public static final int MAX_RETRIES = 3;

    public static void send(Socket socket, String message) throws SocketException {
      int i = 0;
      Long sleepTime;
      int numRetries = 0;
      ThreadLocalRandom threadRandom = ThreadLocalRandom.current();

      while (i < MAX_RETRIES) {

        try {
          OutputStream outputStream = socket.getOutputStream();
          DataOutputStream socketOutputStream = new DataOutputStream(outputStream);
          if (numRetries++ < (MAX_RETRIES - 1)) {
            System.out.println("we're in the retry boys");
            try {
              sleepTime = (numRetries * 1000) + threadRandom.nextLong(1, 1000);
              System.out.println("Sleeping for " + sleepTime + " ms");
              Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
            throw new SocketException("Retires exceeded " + MAX_RETRIES);
          }
          socketOutputStream.writeUTF(message);
          socketOutputStream.flush(); // ??
          System.out.printf("Sent %s to the socket \n", message);
          return;
        } catch (SocketException e) {
          System.err.println(
              "Failed to connect to socket cause connection was closed by the server");
          i++;
          continue;
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
      throw new SocketException("Retires exceeded " + MAX_RETRIES);
    }
  }
}
