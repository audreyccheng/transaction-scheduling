package shield.benchmarks.smallbank;

import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.HashSet;
import java.util.Set;
import shield.benchmarks.utils.BenchmarkTransaction;
import shield.client.DatabaseAbortException;
// import shield.client.RedisPostgresClient;
import shield.client.schema.Table;
import shield.benchmarks.utils.Generator;

/**
 * ConsolidateTransaction represents moving all the funds from one customer to another.
 * It reads the balances for both accounts of customer N1, then sets both to zero, and finally
 * increases the checking balance for N2 by the sum of N1’s previous balances
 */
public class ConsolidateTransaction extends BenchmarkTransaction{
  private SmallBankExperimentConfiguration config;
  private long txn_id;
  private Integer custId1;
  private Integer custId2;

  public ConsolidateTransaction(SmallBankGenerator generator, int custId1, int custId2, long txn_id) {
    this.txn_id = txn_id;
    this.custId1= custId1;
    this.custId2 = custId2;
    this.client = generator.getClient();
    this.config = generator.getConfig();
}



  @Override
  public boolean tryRun() {
    try {

//      System.out.println("Amalgamate");

      List<byte[]> results;
      byte[] rowSavingsCus1;
      byte[] rowCheckingsCus1;
      byte[] rowCheckingsCus2;
      Integer balCC2;
      Integer balCC1;
      Integer balSC1;
      Integer total;

      Table checkingsTable = client.getTable(SmallBankConstants.kCheckingsTable);
      Table savingsTable = client.getTable(SmallBankConstants.kSavingsTable);
      client.startTransaction();

      int type = this.custId1 + 101;
      if (this.custId1 > 39) {
          type = 0;
      }
      if (this.config.SCHEDULE && type != 0) {
          // System.out.println("Scheduling cluster: " + type);
          client.scheduleTransaction(type);
      }

      // Get Account
      results = client.readAndExecute(SmallBankConstants.kAccountsTable, custId1.toString()); //, SmallBankTransactionType.AMALGAMATE.ordinal(), this.txn_id);
    //   results = client.readAndExecute(SmallBankConstants.kAccountsTable, custId2.toString()); //, SmallBankTransactionType.AMALGAMATE.ordinal(), this.txn_id);

      if (results.get(0).equals("")) { // || results.get(1).equals("")) {
          // Invalid customer ids
        client.commitTransaction();
        return true;
      }
      results = client.readAndExecute(SmallBankConstants.kCheckingsTable, custId1.toString()); //, SmallBankTransactionType.AMALGAMATE.ordinal(), this.txn_id);
      rowCheckingsCus1 = results.get(0);
      results = client.readAndExecute(SmallBankConstants.kSavingsTable, custId1.toString()); //, SmallBankTransactionType.AMALGAMATE.ordinal(), this.txn_id);
      rowSavingsCus1 = results.get(0);
    //   results = client.readAndExecute(SmallBankConstants.kCheckingsTable, custId2.toString()); //, SmallBankTransactionType.AMALGAMATE.ordinal(), this.txn_id);
    //   rowCheckingsCus2 = results.get(0);

      balCC1 = (Integer) checkingsTable.getColumn("C_BAL", rowCheckingsCus1);
      balSC1= (Integer) savingsTable.getColumn("S_BAL", rowSavingsCus1);
//      balCC2 = (Integer) checkingsTable.getColumn("C_BAL", rowCheckingsCus2);
      total = balSC1 + balCC1;
//      balCC2+=total;
      balCC1 = 0;
      balSC1 = 0;

        int num_custs = Generator.generateInt(1,10);
        Set<Integer> set = new HashSet<Integer>();
        set.add(custId1);
       for (int i = 0 ; i < num_custs; i++) {
        int cust = Generator.generateInt(50,config.NB_ACCOUNTS);
        while (set.contains(cust)) {
            cust = Generator.generateInt(50,config.NB_ACCOUNTS);
        }
        set.add(cust);

        results = client.readAndExecute(SmallBankConstants.kCheckingsTable, Integer.toString(cust)); //, SmallBankTransactionType.AMALGAMATE.ordinal(), this.txn_id);
        // rowCheckingsCus1 = results.get(0);
        results = client.readAndExecute(SmallBankConstants.kSavingsTable, Integer.toString(cust)); //, SmallBankTransactionType.AMALGAMATE.ordinal(), this.txn_id);
        // rowSavingsCus1 = results.get(0);
        results = client.readAndExecute(SmallBankConstants.kCheckingsTable, Integer.toString(cust)); //, SmallBankTransactionType.AMALGAMATE.ordinal(), this.txn_id);
        // rowCheckingsCus2 = results.get(0);
       }


      checkingsTable.updateColumn("C_BAL", balCC1, rowCheckingsCus1);
      savingsTable.updateColumn("S_BAL", balSC1, rowSavingsCus1);
//      checkingsTable.updateColumn("C_BAL", balCC2, rowCheckingsCus2);
      client.writeAndExecute(SmallBankConstants.kCheckingsTable, custId1.toString(), rowCheckingsCus1); //, SmallBankTransactionType.AMALGAMATE.ordinal(), this.txn_id);
      client.writeAndExecute(SmallBankConstants.kSavingsTable, custId1.toString(), rowSavingsCus1); //, SmallBankTransactionType.AMALGAMATE.ordinal(), this.txn_id);
    //   client.writeAndExecute(SmallBankConstants.kCheckingsTable, custId2.toString(), rowCheckingsCus2); //, SmallBankTransactionType.AMALGAMATE.ordinal(), this.txn_id);

      client.commitTransaction();

      return true;

    } catch (DatabaseAbortException e) {
      return false;
    }

  }
}
