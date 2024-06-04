import java.io.*;
import java.net.*;

public class ClientSync {

  // public static void main(String[] args) {
  //     int port = 6667;
  //
  //     List<Callable<Void>> tasks = new ArrayList<>();
  //     for (int i = 0; i < 10; i++) {
  //         int clientId = i;
  //         try (Socket socket = new Socket("localhost", port)) {
  //             System.out.println("Connected to server from Client " + clientId);
  //
  //             ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
  //             AtomicEvent event = new AtomicEvent("A");
  //             out.writeObject(event);
  //             System.out.println("Sent event from Client " + clientId);
  //
  //             try {
  //               Thread.sleep(3000);
  //             } catch (InterruptedException e) {
  //               System.out.println("Thread sleep interrupted");
  //               e.printStackTrace();
  //             }
  //
  //         } catch (ConnectException e) {
  //             System.out.println("Server is not running. Please start the server and try
  // again.");
  //             e.printStackTrace();
  //         } catch (IOException e) {
  //             e.printStackTrace();
  //         }
  //     }
  // }
}
