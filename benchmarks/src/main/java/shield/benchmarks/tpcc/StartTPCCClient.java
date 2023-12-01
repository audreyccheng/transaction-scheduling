package shield.benchmarks.tpcc;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;

import org.json.simple.parser.ParseException;
import shield.benchmarks.tpcc.utils.TPCCConstants;
import shield.benchmarks.tpcc.utils.TPCCConstants.Transactions;
import shield.benchmarks.utils.CacheStats;
import shield.benchmarks.utils.ClientUtils;
import shield.benchmarks.utils.StatisticsCollector;
import shield.benchmarks.utils.TrxStats;
import shield.client.ClientTransaction;
import shield.client.DatabaseAbortException;
import shield.client.ClientBase;
// import shield.client.RedisPostgresClient;

/**
 * Simulates a client that runs a YCSB workload, the parameters of the YCSB workload are configured
 * in the json file passed in as argument
 *
 * @author ncrooks
 */
public class StartTPCCClient {

    public static class BenchmarkRunnable implements Runnable {
        public int threadNumber;
        public TPCCExperimentConfiguration tpcConfig;
        public String expConfigFile;
        public HashMap<TPCCConstants.Transactions, TrxStats> trxStats;
        public HashMap<String, Integer> custStats;
        // public Map<Long, ReadWriteLock> keyLocks;
        // public long prefetchMemoryUsage;

        BenchmarkRunnable(int threadNumber, TPCCExperimentConfiguration tpcConfig, String expConfigFile) {
            this.threadNumber = threadNumber;
            this.tpcConfig = tpcConfig;
            this.expConfigFile = expConfigFile;
            // this.keyLocks = keyLocks;
            // this.prefetchMemoryUsage = -1;
        }

        @Override
        public void run() {
            StatisticsCollector stats;
            TPCGenerator tpccGenerator;
            long beginTime;
            long expiredTime;
            int nbExecuted = 0;
            int nbAbort = 0;
            boolean success = false;
            ClientTransaction ongoingTrx;
            int measurementKey = 0;
            boolean warmUp;
            boolean warmDown;

            stats = new StatisticsCollector(tpcConfig.RUN_NAME + "_thread" + this.threadNumber);

            try {
                ClientBase client = ClientUtils.createClient(tpcConfig.CLIENT_TYPE, expConfigFile);
                // client.setThreadNumber(this.threadNumber);
                client.registerClient();

                System.out.println("Client registered");

                tpccGenerator = new TPCGenerator(client, tpcConfig);

                System.out.println("Begin Client " + client.getBlockId() + System.currentTimeMillis());

                beginTime = System.currentTimeMillis();
                expiredTime = 0;
                warmUp = true;
                warmDown = false;

                Random ran = new Random();

                int totalRunningTIme = ((tpcConfig.EXP_LENGTH + tpcConfig.RAMP_DOWN + tpcConfig.RAMP_UP) * 1000);

                while (expiredTime < totalRunningTIme) {
//                    System.out.println(warmUp + " " + warmDown + " " + expiredTime);

                    if (tpcConfig.USE_THINK_TIME) Thread.sleep(ran.nextInt(tpcConfig.THINK_TIME));

                    if (!warmUp && !warmDown) {
                        measurementKey = StatisticsCollector.addBegin(stats);
                    }
                    // System.out.println("runNextTransaction");
                    tpccGenerator.runNextTransaction();

                    if (!warmUp && !warmDown) {
                        StatisticsCollector.addEnd(stats, measurementKey);
                    }
                    nbExecuted++;
                    expiredTime = System.currentTimeMillis() - beginTime;
                    if (expiredTime > tpcConfig.RAMP_UP * 1000)
                        warmUp = false;
                    if ((totalRunningTIme - expiredTime) < (tpcConfig.RAMP_DOWN * 1000))
                        warmDown = true;

//                    System.out.println("[Executed] " + nbExecuted + " " + expiredTime + " ");
                }

                // client.requestExecutor.shutdown();
                // while (!client.requestExecutor.isTerminated()) {
                // }

                // tpccGenerator.printStats();
                trxStats = tpccGenerator.getTrxStats();
                custStats = tpccGenerator.getCustStats();
                // prefetchMemoryUsage = client.getPrefetchMapSize();
            } catch (Exception e) {}
        }
    }

  public static void main(String[] args) throws InterruptedException,
      IOException, ParseException, DatabaseAbortException, SQLException {

    String expConfigFile;
    TPCCExperimentConfiguration tpcConfig;

    if (args.length != 1) {
      System.err.println(
          "Incorrect number of arguments: expected <expConfigFile.json>");
    }
    // Contains the experiment paramaters
    expConfigFile = args[0];
    tpcConfig = new TPCCExperimentConfiguration(expConfigFile);
    // Map<Long, ReadWriteLock> keyLocks = new ConcurrentHashMap<>(); // only make one lock map for all clients

    System.out.println("Number of loader threads " + tpcConfig.NB_LOADER_THREADS);

    if (tpcConfig.MUST_LOAD_KEYS) {
      System.out.println("Begin loading data");
        TPCLoader.loadData(tpcConfig, expConfigFile);
    }

    System.out.println("Data loaded");

      Thread[] threads = new Thread[tpcConfig.NB_CLIENT_THREADS];
     BenchmarkRunnable[] runnables = new BenchmarkRunnable[tpcConfig.NB_CLIENT_THREADS];
      for (int i = 0; i < tpcConfig.NB_CLIENT_THREADS; i++) {
          runnables[i] = new  BenchmarkRunnable(i, tpcConfig, expConfigFile);
          threads[i] = new Thread(runnables[i]);
          threads[i].start();
      }

      HashMap<TPCCConstants.Transactions, TrxStats> combinedStats = new HashMap<>();
      HashMap<String, Integer> combinedCustStats = new HashMap<>();
    //   long totalPrefetchMemoryUsage = 0;
      for (int i = 0; i < threads.length; i++) {
          threads[i].join();

          HashMap<TPCCConstants.Transactions, TrxStats> threadStats = runnables[i].trxStats;
          threadStats.forEach((txn, stat) -> {
              if (!combinedStats.containsKey(txn)) combinedStats.put(txn, new TrxStats());
              combinedStats.get(txn).mergeTxnStats(stat);
          });

          HashMap<String, Integer> tStats = runnables[i].custStats;
          tStats.forEach((k, c) -> {
              if (!combinedCustStats.containsKey(k)) {
                combinedCustStats.put(k, c);
              } else {
                int count = combinedCustStats.get(k);
                combinedCustStats.put(k, count + c);
              }

          });
        //   totalPrefetchMemoryUsage += runnables[i].prefetchMemoryUsage;
      }

    //   System.out.println("THREADS COMBINED STATS");

      // Over how long a period the statistics were taken from
      System.out.println("Benchmark duration: " + tpcConfig.EXP_LENGTH);
      combinedStats.forEach((tType,stat) -> System.out.println("[STAT] " + tType + " " +  stat.getStats()));
      combinedStats.forEach((tType,stat) -> System.out.println("[STAT] " + tType + " " +  stat.getAvgLatency()));

      // System.out.print("Customers: ");
      // List<Integer> vals = new ArrayList<>(combinedCustStats.values());
      // Collections.sort(vals, Collections.reverseOrder());
      // for (Integer v : vals) {
      //     if (v > 1)
      //         System.out.print(v + " ");
      // }
      // System.out.println("");

      System.out.println();
      long txnsExecuted = combinedStats.values().stream().map(TrxStats::getExecuteCount).reduce(0L, Long::sum);
      System.out.println("Average throughput: " + txnsExecuted / tpcConfig.EXP_LENGTH + " txn/s");
      double avg = ((float) combinedStats.values().stream().map(TrxStats::getTimeExecuted).reduce(0L, Long::sum)) / txnsExecuted;
      System.out.println("Average latency: " + avg + "ms");

      List<Long> lats = new LinkedList<>();
      for (TrxStats ts : combinedStats.values()) {
        lats.addAll(ts.getLatencies());
      }
      int index50 = (int) (.50 * lats.size());
      int index99 = Math.min((int) (.99 * lats.size()), lats.size() - 1);
      int index999 = Math.min((int) (.999 * lats.size()), lats.size() - 1);
      Collections.sort(lats);
      Object[] latsA = lats.toArray();

      double sumDiffsSquared = 0.0;
      for (Long value : lats) {
          double diff = value - avg;
          diff *= diff;
          sumDiffsSquared += diff;
      }
      double var = (sumDiffsSquared  / (lats.size()-1));
      System.out.println("Latency variance: " + var);

      System.out.println("50th percentile latency: " + latsA[index50]);
      System.out.println("99th percentile latency: " + latsA[index99]);
      System.out.println("99.9th percentile latency: " + latsA[index999]);
      System.out.println("Max latency: " + latsA[lats.size() - 1]);
      long numAborts = combinedStats.values().stream().map(TrxStats::getTotAborts).reduce(0L, Long::sum);
      System.out.println("Total number of aborts: " + numAborts + " with abort rate: " + ((float) numAborts) / (numAborts + txnsExecuted));
    //   System.out.println("Prefetching memory usage ~" + totalPrefetchMemoryUsage + " bytes");

      System.out.println();
    //   CacheStats.printReport();
      System.exit(0);
    }

}
