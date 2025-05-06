import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

//private static final String DB_URL = "jdbc:mysql://localhost:3306/Metro_Star_Schema";
//private static final String USER = "root";
//private static final String PASSWORD = "raemo980/";

public class Main {
    public static void main(String[] args) {
        // Shared BlockingQueue for communication
        BlockingQueue<String[]> sharedQueue = new ArrayBlockingQueue<>(10);

        // Pass sharedQueue to threads
        Thread generateStreamThread = new Thread(new GenerateStream(sharedQueue));
        Thread meshJoinThread = new Thread(new Meshjoin(sharedQueue));

        // Start threads
        generateStreamThread.start();
        meshJoinThread.start();

        // Wait for threads to complete
        try {
            generateStreamThread.join();
            meshJoinThread.join();
        } catch (InterruptedException e) {
            System.err.println("Main thread interrupted!");
        }


    }
}

