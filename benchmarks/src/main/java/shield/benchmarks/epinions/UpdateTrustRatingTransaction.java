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

public class UpdateTrustRatingTransaction extends BenchmarkTransaction {
    private EpinionsExperimentConfiguration config;
    private int uid;
    private int uid2;
    private int trust;
    private long txn_id;

    public UpdateTrustRatingTransaction(EpinionsGenerator generator, int uid, int uid2, int trust,
                                         long txn_id) {
        this.uid = uid;
        this.uid2 = uid2;
        this.trust = trust;
        this.txn_id = txn_id;
        this.client = generator.getClient();
        this.config = generator.getConfig();
    }

    public boolean tryRun() {
        try {
            List<byte[]> results;
            byte[] rowTrust1;

            Table trustTable = client.getTable(EpinionsConstants.kTrustTable);

            client.startTransaction();

            int type = this.uid + 101 + 20;
            if (this.uid > 19) {
                type = 0;
            }
            if (this.config.SCHEDULE && type != 0) {
                // System.out.println("Scheduling cluster: " + type);
                client.scheduleTransaction(type);
            }

            results = client.readForUpdateAndExecute(EpinionsConstants.kTrustTable, Integer.toString(uid)); //,
                    // EpinionsConstants.Transactions.UPDATETRUSTRATING.ordinal(), this.txn_id);

            if (results != null) {
                rowTrust1 = results.get(0);
//                trustTable.updateColumn("T_TU_ID", uid2, rowTrust1);
//                trustTable.updateColumn("T_TRUST", trust, rowTrust1);

                client.writeAndExecute(EpinionsConstants.kUserTable, Integer.toString(uid), rowTrust1); //,
                        // EpinionsConstants.Transactions.UPDATETRUSTRATING.ordinal(), this.txn_id);
            }

            client.commitTransaction();

            return true;

        } catch (DatabaseAbortException e) {
            return false;
        }
    }

}
