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

public class UpdateItemTitleTransaction extends BenchmarkTransaction {
    private EpinionsExperimentConfiguration config;
    private int iid;
    private String title;
    private long txn_id;

    public UpdateItemTitleTransaction(EpinionsGenerator generator, int iid, String title, long txn_id) {
        this.iid = iid;
        this.title = title;
        this.txn_id = txn_id;
        this.client = generator.getClient();
        this.config = generator.getConfig();
    }

    public boolean tryRun() {
        try {
            List<byte[]> results;
            byte[] rowItem1;

            Table itemTable = client.getTable(EpinionsConstants.kItemTable);

            client.startTransaction();

            int type = this.iid + 101;
            if (this.iid > 19) {
                type = 0;
            }
            if (this.config.SCHEDULE && type != 0) {
                // System.out.println("Scheduling cluster: " + type);
                client.scheduleTransaction(type);
            }

            // System.out.println("UpdateItemTitleTransaction iid: " + iid);
            results = client.readForUpdateAndExecute(EpinionsConstants.kItemTable, Integer.toString(iid)); //,
                    // EpinionsConstants.Transactions.UPDATEITEMTITLE.ordinal(), this.txn_id);

            if (results != null) {
                rowItem1 = results.get(0);

//                itemTable.updateColumn("I_NAME", title, rowItem1);

                client.writeAndExecute(EpinionsConstants.kItemTable, Integer.toString(iid), rowItem1); //,
                        // EpinionsConstants.Transactions.UPDATEITEMTITLE.ordinal(), this.txn_id);
            }

            client.commitTransaction();

            return true;

        } catch (DatabaseAbortException e) {
            return false;
        }
    }
}
