package io.millesabords.demo.streamingsql;

import net.andreinc.mockneat.MockNeat;
import net.andreinc.mockneat.types.enums.IPv4Type;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Random;

public class LogProvider {

    private static final String[] STATUS = {
            "200", "403", "404", "500"
    };

    private static final String[] URLS = {
            "/home", "/products", "/commands", "/help"
    };

    private static ServerSocket serverSocket;

    private static Socket clientSocket;

    public static void main(final String[] args) {

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            closeConnection();
        }));

        final MockNeat mock = MockNeat.threadLocal();

        try {
            serverSocket = new ServerSocket(5000);
            System.out.println("Waiting for a connection...");

            clientSocket = serverSocket.accept();
            System.out.println("New connection !");

            final Random random = new Random();
            final PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);

            while (true) {
                out.println(String.join("\t",
                        Long.toString(System.currentTimeMillis()),
                        mock.ipv4s().type(IPv4Type.CLASS_A).val(),
                        mock.from(URLS).val(),
                        mock.from(STATUS).val(),
                        mock.ints().range(100, 5000).valStr()
                        ));
                Thread.sleep(random.nextInt(10) + 50);
            }
        }
        catch (final Exception ex) {
            ex.printStackTrace();
        }
    }

    private static void closeConnection() {
        System.out.println("Stopping server");
        try {
            if (clientSocket != null) {
                clientSocket.close();
            }

            if (serverSocket != null) {
                serverSocket.close();
            }
        }
        catch (final IOException e) {
        }
    }
}
