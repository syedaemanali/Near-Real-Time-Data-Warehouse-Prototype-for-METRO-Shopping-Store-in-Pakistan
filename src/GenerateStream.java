import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.concurrent.BlockingQueue;

public class GenerateStream implements Runnable {
    private static final int BUFFER_SIZE = 50; // Number of transactions per batch
    private BlockingQueue<String[]> sharedQueue; // Shared queue with Main
    private String[] streamBuffer = new String[BUFFER_SIZE];
    private int bufferIndex = 0;

    // List to store all unique records
    private ArrayList<String> allRecords = new ArrayList<>();

    // Constructor
    public GenerateStream(BlockingQueue<String[]> sharedQueue) {
        this.sharedQueue = sharedQueue;
    }

    @Override
    public void run() {
        try (Scanner readTransactions = new Scanner(new File("C:/Users/syeda/Downloads/transactions - transactions.csv"))) {
            readTransactions.useDelimiter("\n"); // Set newline as the delimiter

            // Skip the header row
            if (readTransactions.hasNext()) {
                readTransactions.next();
            }

            System.out.println("Starting transaction stream...");

            while (readTransactions.hasNext()) {
                // Read the next transaction record
                String record = readTransactions.next().trim();

                // Check if the transaction is already processed
                if (!allRecords.contains(record)) {
                    allRecords.add(record);

                    streamBuffer[bufferIndex] = record;
                    bufferIndex++;
                }

                // If the buffer is full, add it to the shared queue
                if (bufferIndex == BUFFER_SIZE)
                {
                    sharedQueue.put(streamBuffer.clone());
                    bufferIndex = 0; // Reset the index for the next batch
                }
            }

            // Process remaining transactions in the buffer
            if (bufferIndex > 0) {
                sharedQueue.put(streamBuffer.clone());
            }

            //System.out.println("Transaction stream completed.");
        } catch (FileNotFoundException e) {
            System.err.println("Transaction file not found!");
        } catch (InterruptedException e) {
            System.err.println("GenerateStream interrupted!");
        }
    }
}
