package shield.benchmarks.utils;

import java.util.Random;
import shield.client.ClientBase;

public abstract class BenchmarkTransaction {


  /**
   * Reference to client (will actually
   * execute the transaction)
   */
  protected ClientBase client;

  Random random = new Random();

  /**
   * Executes the transaction, first generating
   * input and then retrying as many times as is
   * necessary
   */
  public int run() {
      boolean success = false;
      int nbAborts = 0 ;
      double backOff = random.nextInt(5) + 1;
      double maxBackoff = 10000;
      long startTime = System.currentTimeMillis();
      while (!success) {
        success = tryRun();
        // System.out.println("BenchmarkTransaction tryRun success: " + success);
        if (!success) {
          // System.out.println("Aborting!");
          nbAborts++;

          // Backoff
          if (client.getConfig().USE_BACKOFF) {
            try {
              if (client.getConfig().USE_MAX_BACKOFF) {
                // backOff = Math.min(random.nextInt((int) Math.pow(2, nbAborts)), maxBackoff);
                backOff = Math.min((int) (backOff * 2 * (1.0 + random.nextDouble())), maxBackoff);
              } else {
                backOff = (int) (backOff * 2 * (1.0 + random.nextDouble()));
                // backOff = random.nextInt((int) Math.pow(2, nbAborts));
              }
              // System.out.println("Backing Off " + backOff + " nbAborts: " + nbAborts);
              Thread.sleep((int) backOff);
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
          }
        }

        /*if (nbAborts>10) {
          System.err.println("Giving up this transaction");
          break; // Hack
        } */
      }
      long estimatedTime = System.currentTimeMillis() - startTime;
      // if (nbAborts > 0) {
      //   System.out.println("Aborted txn took: " + estimatedTime + " nbAborts: " + nbAborts);
      // } else if (random.nextInt(100) < 10) {
      //   System.out.println("Normal txn took: " + estimatedTime + " nbAborts: " + nbAborts);
      // }
      return nbAborts;
  }
  /**
   * Attempts to execute one instance of the
   * transaction
   */
  public abstract boolean tryRun();


}
