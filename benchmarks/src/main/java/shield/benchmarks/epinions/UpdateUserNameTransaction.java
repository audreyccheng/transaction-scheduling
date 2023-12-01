package shield.benchmarks.epinions;

import java.util.List;

import shield.benchmarks.epinions.EpinionsGenerator;
import shield.benchmarks.epinions.SerializableIDSet;
import shield.benchmarks.smallbank.SmallBankConstants;
import shield.benchmarks.smallbank.SmallBankTransactionType;
import shield.benchmarks.utils.BenchmarkTransaction;
import shield.benchmarks.utils.Generator;
import shield.client.DatabaseAbortException;
// import shield.client.RedisPostgresClient;
import shield.client.ClientBase;
import shield.client.schema.Table;

public class UpdateUserNameTransaction extends BenchmarkTransaction {
    private EpinionsExperimentConfiguration config;
    private int uid;
    private String title;
    private long txn_id;

    public UpdateUserNameTransaction(EpinionsGenerator generator, int uid, String title, long txn_id) {
        this.uid = uid;
        this.title = title;
        this.txn_id = txn_id;
        this.client = generator.getClient();
        this.config = generator.getConfig();
    }

    public boolean tryRun() {
        try {
            List<byte[]> results;
            byte[] rowUser1;

//            System.out.println("UpdateUserNameTransaction user " + this.uid);

            Table userTable = client.getTable(EpinionsConstants.kUserTable);

            client.startTransaction();

            int type = this.uid + 101 + 20;
            if (this.uid > 19) {
                type = 0;
            }
            if (this.config.SCHEDULE && type != 0) {
                // System.out.println("Scheduling cluster: " + type);
                client.scheduleTransaction(type);
            }

            results = client.readForUpdateAndExecute(EpinionsConstants.kUserTable, Integer.toString(uid)); //,
                    // EpinionsConstants.Transactions.UPDATEUSERNAME.ordinal(), this.txn_id);

            if (results != null) {
                rowUser1 = results.get(0);
//                userTable.updateColumn("U_NAME", title, rowUser1);

                client.writeAndExecute(EpinionsConstants.kUserTable, Integer.toString(uid), rowUser1); //,
                        // EpinionsConstants.Transactions.UPDATEUSERNAME.ordinal(), this.txn_id);
            }

            client.commitTransaction();

//            System.out.println("UpdateUserNameTransaction updated name " + this.title);

            return true;

        } catch (DatabaseAbortException e) {
            return false;
        }
    }
}
