package shield.benchmarks.readwrite;

import java.util.Arrays;
import java.util.List;
import java.util.HashSet;
import java.util.Set;
import shield.benchmarks.utils.BenchmarkTransaction;
import shield.benchmarks.utils.Generator;
import shield.client.DatabaseAbortException;
import shield.client.ClientBase;
import shield.client.schema.Table;

/**
 * a parameterized transaction that represents calculating the total balance for
 * a customer with name N. It returns the sum of savings and checking balances for the specified
 * customer
 */
public class YCSBTransaction extends BenchmarkTransaction{
  private final RWExperimentConfiguration config;
  private Set<Integer> keys;

  public YCSBTransaction(RWGenerator generator, Set<Integer> keys, long txn_id) {
    this.keys = keys;
    this.client = generator.getClient();
    this.config = generator.getConfig();
  }

  @Override
  public boolean tryRun() {
  try {

//      System.out.println("ReadZ Transaction");

    List<byte[]> results;

    client.startTransaction();

    Integer[] keysN = keys.toArray(new Integer[keys.size()]);

    if (Generator.generateInt(0,100) < config.READ_PERCENT) {
        results = client.readAndExecute("", keysN[0].toString());
    } else {


    Arrays.sort(keysN);
    int wk = keysN[0];

    int type = wk + 101;
    if (wk > 39) {
        type = 0;
    }

    if (this.config.SCHEDULE && type != 0) {
        // System.out.println("type: " + wk);
        client.scheduleTransaction(type);
    }

    byte[] val = Generator.generateBytes(config.VALUE_SIZE);
    for (Integer k : keys) {
        // if (Generator.generateInt(0,100) < config.READ_PERCENT) {
            results = client.readAndExecute("", k.toString());
        // } else {
        //     results = client.writeAndExecute("", k.toString(), val);
        // }
    }

    results = client.writeAndExecute("", Integer.toString(wk), val);
    }

      client.commitTransaction();
      return true;

    } catch (DatabaseAbortException e) {
      return false;
    }
  }
}
