package shield.benchmarks.readwrite;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.HashSet;
import java.util.Set;
import shield.benchmarks.readwrite.RWConstants.Transactions;
import shield.benchmarks.readwrite.ReadXTransaction;
import shield.benchmarks.readwrite.ReadZTransaction;
import shield.benchmarks.ycsb.utils.ZipfianIntGenerator;

import shield.benchmarks.utils.BenchmarkTransaction;
import shield.benchmarks.utils.Generator;
import shield.benchmarks.utils.TrxStats;
import shield.client.ClientBase;
import shield.client.schema.ColumnInfo;

public class RWGenerator {

  private final RWExperimentConfiguration config;

  private final ClientBase client;
  private final HashMap<Transactions, TrxStats> trxStats;
  private final char[] ALPHANUM =  "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz".toCharArray();
  private final int nbKeys;
  private static long txn_id = 0;
  private ZipfianIntGenerator keyRecord;

  public RWGenerator(ClientBase client, RWExperimentConfiguration config) {
    this.config = config;
    this.client = client;
    this.trxStats = new HashMap<>();
    for (RWConstants.Transactions tType: RWConstants.Transactions.values()) {
      trxStats.put(tType, new TrxStats());
    }
    this.nbKeys = (int) (config.NB_KEYS);
    this.keyRecord = new ZipfianIntGenerator(config.NB_KEYS, config.YCSB_ZIPF);
  }

  public ClientBase getClient() {
    return client;
  }

  public void runNextTransaction() {
    int x = Generator.generateInt(0,100);
    int nbAborts;
    long begin = System.currentTimeMillis();
    long end = 0;
    BenchmarkTransaction trx;

    if (x < config.PROB_TRX_YCSB) {
      trx = GenerateYCSBInput();
      nbAborts = trx.run();
      end = System.currentTimeMillis();
      trxStats.get(Transactions.READ_Z_WRITE_X).addTransaction(nbAborts,end-begin);
    } else if (x < config.PROB_TRX_TAOBENCH) {
       trx = GenerateTaoBenchInput();
//      System.out.println("[" + Transactions.AMALGAMATE+ "] Begin");
      nbAborts = trx.run();
      end = System.currentTimeMillis();
      trxStats.get(Transactions.READ_X_WRITE_X).addTransaction(nbAborts, end-begin);
//      System.out.println("[" + Transactions.AMALGAMATE + "] End");
    } else if (x < config.PROB_TRX_READX) {
      trx = GenerateReadXInput();
//      System.out.println("[" + Transactions.AMALGAMATE+ "] Begin");
      nbAborts = trx.run();
      end = System.currentTimeMillis();
      trxStats.get(Transactions.READ_X_WRITE_X).addTransaction(nbAborts, end-begin);
//      System.out.println("[" + Transactions.AMALGAMATE + "] End");
    } else {
      trx = GenerateReadZInput();
//      System.out.println("[" + Transactions.WRITE_CHECK+ "] Begin");
      nbAborts = trx.run();
      end = System.currentTimeMillis();
//       System.out.println("[" + Transactions.WRITE_CHECK+ "] End");
      trxStats.get(Transactions.READ_Z_WRITE_X).addTransaction(nbAborts,end-begin);
    }
  }

  public TaoBenchTransaction GenerateTaoBenchInput() {
    return new TaoBenchTransaction(this, this.keyRecord.nextValue(), txn_id++);
  }

  public ReadXTransaction GenerateReadXInput() {
    int X = generateXKey();
    int Z = generateXKey();// generateZKey();
    return new ReadXTransaction(this, X, Z, txn_id++);
  }

  public ReadZTransaction GenerateReadZInput() {
    int X = generateXKey();
    int Z = generateZKey();
    return new ReadZTransaction(this, X, Z, txn_id++);
  }

  public YCSBTransaction GenerateYCSBInput() {
    Set<Integer> keys = new HashSet<Integer>();
    while (keys.size() < config.TRX_SIZE) {
      int key = this.keyRecord.nextValue();
      if (!keys.contains(key)) {
        keys.add(key);
      }
    }
    return new YCSBTransaction(this, keys, txn_id++);
  }

  public HashMap<Transactions, TrxStats> getTrxStats() {
    return trxStats;
  }

  public char RandCharNum(boolean numOnly) {
    int x = Generator.generateInt(0,numOnly?10:26);
    return ALPHANUM[x];
  }

  /**
   * Generates a random string of size between min and max, and optinally consisting
   * of numbers only
   *
   * @param num_only
   * @return
   */
  public String RandString(int min, int max, boolean num_only) {
    StringBuffer bf = new StringBuffer();
    int len = Generator.generateInt(min, max);
    for (int i = 0; i < len; ++i) {
      bf.append(RandCharNum(num_only));
    }
    return bf.toString();
  }

  public int generateXKey() {
      return Generator.generateInt(0,config.NB_HOT_KEYS);
  }

  public int generateZKey() {
      return Generator.generateInt(config.NB_HOT_KEYS,config.NB_HOT_KEYS*2);
  }

  public int generateKey() {
      return Generator.generateInt(config.NB_HOT_KEYS*2,config.NB_KEYS);
  }

  public void printStats() {
    trxStats.forEach((tType,stat) -> System.out.println("[STAT] " + tType + " " +  stat.getStats()));
  }

  public RWExperimentConfiguration getConfig() {
        return config;
    }


}
