package shield.benchmarks.smallbank;

import java.util.List;
import shield.benchmarks.utils.BenchmarkTransaction;
import shield.client.DatabaseAbortException;
// import shield.client.RedisPostgresClient;
import shield.client.schema.Table;

/**
 * Represents making a deposit or withdrawal on the savings account.
 * It increases or decreases the savings balance by V for the specified customer. If the
 * name N is not found in the table or if the transaction would result in a negative savings balance
 * for the customer, the transaction will rollback
 */
public class TransactSavingsTransaction extends BenchmarkTransaction{
  private SmallBankExperimentConfiguration config;
  private Integer cust;
  private Integer amount;
  private long txn_id;

  public TransactSavingsTransaction(SmallBankGenerator generator, int cust, int amount, long txn_id) {
    this.txn_id = txn_id;
    this.cust= cust;
    this.amount = amount;
    this.client = generator.getClient();
    this.config = generator.getConfig();
    }



  @Override
  public boolean tryRun() {
    try {

//      System.out.println("Transact Savings");

      List<byte[]> results;
      byte[] row;
      Integer balance;

      Table savingsTable= client.getTable(SmallBankConstants.kSavingsTable);
      client.startTransaction();

      int type = this.cust + 101;
      if (this.cust > 39) {
          type = 0;
      }
      if (this.config.SCHEDULE && type != 0) {
          // System.out.println("Scheduling cluster: " + type);
          client.scheduleTransaction(type);
      }

      // Get Account
      results = client.readAndExecute(SmallBankConstants.kAccountsTable, cust.toString()); //, SmallBankTransactionType.TRANSACT_SAVINGS.ordinal(), this.txn_id);

      if (results.get(0).equals("")){
          // Invalid customer ids
          throw new DatabaseAbortException();
      }
      results = client.readAndExecute(SmallBankConstants.kSavingsTable, cust.toString()); //, SmallBankTransactionType.TRANSACT_SAVINGS.ordinal(), this.txn_id);
      row = results.get(0);

      balance = (Integer) savingsTable.getColumn("S_BAL", row);

      if (balance< amount) {
          // Insufficient money
//        System.out.println("Aborting, insuffient money");
        //throw new DatabaseAbortException();
        client.commitTransaction();
        return true;
      }

      savingsTable.updateColumn("S_BAL", balance - amount, row);
      client.writeAndExecute(SmallBankConstants.kSavingsTable, cust.toString(), row); //, SmallBankTransactionType.TRANSACT_SAVINGS.ordinal(), this.txn_id);
      client.commitTransaction();

      return true;

    } catch (DatabaseAbortException e) {
      return false;
    }

  }
}
