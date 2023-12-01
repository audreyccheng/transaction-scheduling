package shield.benchmarks.epinions;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;

import org.json.simple.parser.ParseException;
import shield.benchmarks.utils.CacheStats;
import shield.benchmarks.utils.ClientUtils;
import shield.benchmarks.utils.StatisticsCollector;
import shield.benchmarks.utils.TrxStats;
import shield.client.ClientTransaction;
import shield.client.DatabaseAbortException;
import shield.client.ClientBase;
// import shield.client.RedisPostgresClient;

public class StartEpinionsTrxClient {

    public static class BenchmarkRunnable implements Runnable {

        public int threadNumber;
        public EpinionsExperimentConfiguration tpcConfig;
        public String expConfigFile;
        public HashMap<EpinionsConstants.Transactions, TrxStats> trxStats;
        // public Map<Long, ReadWriteLock> keyLocks;
        // public long prefetchMemoryUsage;

        BenchmarkRunnable(int threadNumber, EpinionsExperimentConfiguration tpcConfig, String expConfigFile) { //, Map<Long, ReadWriteLock> keyLocks
            this.threadNumber = threadNumber;
            this.tpcConfig = tpcConfig;
            this.expConfigFile = expConfigFile;
            // this.keyLocks = keyLocks;
            // this.prefetchMemoryUsage = -1;
        }

        @Override
        public void run() {
            StatisticsCollector stats;
            EpinionsGenerator epinionsGenerator;
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
                // ClientUtils.createClient(tpcConfig.CLIENT_TYPE,
                //        expConfigFile, keyLocks, 7000 + this.threadNumber, this.threadNumber);
                // client.setThreadNumber(this.threadNumber);
                client.registerClient();

                System.out.println("Client registered");

                epinionsGenerator = new EpinionsGenerator(client, tpcConfig);

                System.out.println("Begin Client " + client.getBlockId() + System.currentTimeMillis());

                // Trying to minimise timing differences
                //Thread.sleep(client.getBlockId() * 500);

                beginTime = System.currentTimeMillis();
                expiredTime = 0;
                warmUp = true;
                warmDown = false;

                int totalRunningTime = (tpcConfig.EXP_LENGTH + tpcConfig.RAMP_UP + tpcConfig.RAMP_DOWN) * 1000;

                while (expiredTime < totalRunningTime) {
                    if (!warmUp && !warmDown) {
                        measurementKey = StatisticsCollector.addBegin(stats);
                    }
                    epinionsGenerator.runNextTransaction();
                    if (!warmUp && !warmDown) {
                        StatisticsCollector.addEnd(stats, measurementKey);
                    }
                    nbExecuted++;
                    expiredTime = System.currentTimeMillis() - beginTime;
                    if (expiredTime > tpcConfig.RAMP_UP * 1000)
                        warmUp = false;
                    if ((totalRunningTime - expiredTime) < tpcConfig.RAMP_DOWN * 1000)
                        warmDown = true;

//          System.out.println("[Executed] " + nbExecuted + " " + expiredTime + " ");
                }

                // client.requestExecutor.shutdown();
                // while (!client.requestExecutor.isTerminated()) {}

                // epinionsGenerator.printStats();
                trxStats = epinionsGenerator.getTrxStats();
                // prefetchMemoryUsage = client.getPrefetchMapSize();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws InterruptedException,
            IOException, ParseException, DatabaseAbortException, SQLException {

        String expConfigFile;
        EpinionsExperimentConfiguration tpcConfig;

        if (args.length != 1) {
            System.err.println(
                    "Incorrect number of arguments: expected <expConfigFile.json>");
        }
        // Contains the experiment paramaters
        expConfigFile = args[0];
        tpcConfig = new EpinionsExperimentConfiguration(expConfigFile);
        // Map<Long, ReadWriteLock> keyLocks = new ConcurrentHashMap<>(); // only make one lock map for all clients

        System.out.println("Number of loader threads " + tpcConfig.NB_LOADER_THREADS);

        if (tpcConfig.MUST_LOAD_KEYS) {
            System.out.println("Begin loading data");
            EpinionsLoader.loadData(tpcConfig, expConfigFile);
        }

        System.out.println("Data loaded");

        Thread[] threads = new Thread[tpcConfig.NB_CLIENT_THREADS];
        StartEpinionsTrxClient.BenchmarkRunnable[] runnables = new StartEpinionsTrxClient.BenchmarkRunnable[tpcConfig.NB_CLIENT_THREADS];
        for (int i = 0; i < tpcConfig.NB_CLIENT_THREADS; i++) {
            runnables[i] = new StartEpinionsTrxClient.BenchmarkRunnable(i, tpcConfig, expConfigFile); //, keyLocks
            threads[i] = new Thread(runnables[i]);
            threads[i].start();
        }

        HashMap<EpinionsConstants.Transactions, TrxStats> combinedStats = new HashMap<>();

        // long totalPrefetchMemoryUsage = 0;
        for (int i = 0; i < threads.length; i++) {
            threads[i].join();

            HashMap<EpinionsConstants.Transactions, TrxStats> threadStats = runnables[i].trxStats;
            threadStats.forEach((txn, stat) -> {
                if (!combinedStats.containsKey(txn)) combinedStats.put(txn, new TrxStats());
                combinedStats.get(txn).mergeTxnStats(stat);
            });

            // totalPrefetchMemoryUsage += runnables[i].prefetchMemoryUsage;
        }

        // System.out.println("THREADS COMBINED STATS");

        // Over how long a period the statistics were taken from
        System.out.println("Benchmark duration: " + tpcConfig.EXP_LENGTH);
        combinedStats.forEach((tType,stat) -> System.out.println("[STAT] " + tType + " " +  stat.getStats()));
        // combinedStats.forEach((tType,stat) -> System.out.println("[STAT] " + tType + " " +  stat.getAvgLatency()));

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
      System.out.println("Max latency: " + latsA[lats.size() - 1]);long numAborts = combinedStats.values().stream().map(TrxStats::getTotAborts).reduce(0L, Long::sum);
        System.out.println("Total number of aborts: " + numAborts + " with abort rate: " + ((float) numAborts) / (numAborts + txnsExecuted));
        //System.out.println("Prefetching memory usage: ~" + totalPrefetchMemoryUsage + " bytes");

        System.out.println();
        // CacheStats.printReport();

        System.exit(0);
    }
}
