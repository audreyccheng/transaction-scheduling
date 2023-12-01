package shield.benchmarks.readwrite;

import java.util.List;
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
public class ReadZTransaction extends BenchmarkTransaction{
  private final RWExperimentConfiguration config;
  private Integer X;
  private Integer Z;

  public ReadZTransaction(RWGenerator generator, int X, int Z, long txn_id) {
    this.X = X;
    this.Z = Z;
    this.client = generator.getClient();
    this.config = generator.getConfig();
  }

  @Override
  public boolean tryRun() {
  try {

//      System.out.println("ReadZ Transaction");

      List<byte[]> results;

      client.startTransaction();
    if (this.config.SCHEDULE) {
        int type = 1;//this.Z + 1;
        client.scheduleTransaction(type);
      }
      // Get Account
      results = client.readAndExecute("", this.Z.toString());
    //   if (results.get(0).equals("")) {
    //       // Invalid customer ids
    //     System.out.println("Invalid load");
    //     client.commitTransaction();
    //     return true;
    //   }
    //   results = client.readForUpdateAndExecute("", this.X.toString());

      byte[] val = Generator.generateBytes(config.VALUE_SIZE);

      for (int i = 0; i < config.TRX_SIZE; i++) {
       int key = Generator.generateInt(config.NB_HOT_KEYS*2,config.NB_KEYS);
        int x = Generator.generateInt(0,100);
        if (x < (int) (config.READ_RATIO * 100)) {
            results = client.readAndExecute("", Integer.toString(key));
        } else {
            client.writeAndExecute("", Integer.toString(key), val);
        }
      }

      results = client.writeAndExecute("", this.X.toString(), val);

      client.commitTransaction();
      return true;

    } catch (DatabaseAbortException e) {
      return false;
    }
  }
}
