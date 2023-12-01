package shield.benchmarks.utils;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class TrxStats {

  private long executeCount = 0;
  private long maxAborts = 0;
  private long totAborts = 0;
  private long timeExecuted= 0;
  private long numSuccessful = 0;
  private List<Long> latencies= new LinkedList<>();

  public void addTransaction(int nbAborts, long executed) {
    executeCount++;
    maxAborts = nbAborts > maxAborts? nbAborts: maxAborts;
    totAborts+=nbAborts;
    timeExecuted+=executed;
    latencies.add(executed);
  }

  public long getExecuteCount() {
    return executeCount;
  }

  public long getTimeExecuted() {
    return timeExecuted;
  }

  public List<Long> getLatencies() {
    return latencies;
  }

//  public long getLatencyPercentile(double percentile) {
//    int index = (int) (percentile * latencies.size());
//    if (index == latencies.size()) index = latencies.size() - 1;
//    return latencies[index];
//  }
//
//  public long getMaxLatency() {
//    return latencies[latencies.size() - 1];
//  }

  public long getNumSuccessful() {
    return numSuccessful;
  }

  public long getMaxAborts() {
    return maxAborts;
  }

  public long getTotAborts() {
    return totAborts;
  }

  /**
   * Merges stats from a parallel threads. This TrxStats represents the aggregation of all threads.
   * @param stats Stats from a benchmark thread.
   */
  public void mergeTxnStats(TrxStats stats) {
    this.executeCount += stats.executeCount;
    this.maxAborts = Math.max(stats.maxAborts, this.maxAborts);
    this.totAborts += stats.totAborts;

    // Add time executed; not total elapsed time (that would be max()),
    // but total time summed across all threads for avg. latency calculation
    this.timeExecuted += stats.timeExecuted;

    this.latencies.addAll(stats.latencies);
    // Collections.sort(this.latencies);
  }

  public String getStats() {
    return executeCount + " " + totAborts + " " + maxAborts + " " + ((float)timeExecuted/executeCount);
  }

  public String getAvgLatency() {
    long sum = this.latencies.stream().mapToLong(Long::longValue).sum();
    float avg = sum / (float) this.latencies.size();
    return "" + avg;
  }

}
