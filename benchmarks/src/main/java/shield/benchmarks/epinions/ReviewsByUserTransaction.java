package shield.benchmarks.epinions;

import java.util.List;

import shield.benchmarks.epinions.EpinionsGenerator;
import shield.benchmarks.epinions.SerializableIDSet;
import shield.benchmarks.utils.BenchmarkTransaction;
import shield.benchmarks.utils.Generator;
import shield.client.DatabaseAbortException;
// import shield.client.RedisPostgresClient;
import shield.client.ClientBase;
import shield.client.schema.Table;


public class ReviewsByUserTransaction extends BenchmarkTransaction {
    private EpinionsExperimentConfiguration config;
    private int uid;
    private long txn_id;

    public ReviewsByUserTransaction(EpinionsGenerator generator, int uid, long txn_id) {
        this.uid = uid;
        this.txn_id = txn_id;
        this.client = generator.getClient();
        this.config = generator.getConfig();
    }

    public static boolean isEmptyRow(byte[] row) {
        return row.length == 0;
    }

    public boolean tryRun() {
        try {

//            System.out.println("ReviewsByUserTransaction");
            List<byte[]> results;

            Table reviewTable = client.getTable(EpinionsConstants.kReviewTable);
            Table reviewByUIDTable = client.getTable(EpinionsConstants.kReviewByUIDTable);
            Table userTable = client.getTable(EpinionsConstants.kUserTable);

            client.startTransaction();

            results = client.readAndExecute(EpinionsConstants.kReviewByUIDTable,
                    Integer.toString(uid)); //, EpinionsConstants.Transactions.REVIEWSBYUSER.ordinal(), this.txn_id);

            SerializableIDSet ids;
            if (results.size() > 0) {
                // this works because all three tables are structured identically
                byte[] nameRow = results.get(0);
                if (isEmptyRow(results.get(0))) nameRow = reviewByUIDTable.createNewRow(config.PAD_COLUMNS);
                ids = new shield.benchmarks.epinions.SerializableIDSet(config,
                        (String) reviewByUIDTable.getColumn("ID_LIST", nameRow));
            } else {
                ids = new SerializableIDSet(config);
            }
            if (results.size() > 1) {
                for (int i = 1; i < results.size(); i++){
                    byte[] nameRow = results.get(i);
                    if (isEmptyRow(results.get(i))) nameRow = reviewByUIDTable.createNewRow(config.PAD_COLUMNS);
                    ids.intersect(new shield.benchmarks.epinions.SerializableIDSet(config,
                            (String) reviewByUIDTable.getColumn("ID_LIST", nameRow)));
                }
            }

            List<Integer> idList = ids.toList();
            if (idList.size() > 0) {
                for (int i = 0; i < ids.size() - 1; i++) {
                    client.read(EpinionsConstants.kReviewTable, idList.get(i).toString());
                }
                results = client.readAndExecute(EpinionsConstants.kReviewTable,
                        idList.get(idList.size()-1).toString());

                if (results != null) {
                    for (byte[] res : results) {
                        if (isEmptyRow(res)) {
                            //                        client.abortTransaction();
                            //                        return false; // user does not actually exist so error out.
                            continue;
                        }
                    }
                }
            }

            client.readAndExecute(EpinionsConstants.kUserTable, Integer.toString(uid)); //,
                    // EpinionsConstants.Transactions.REVIEWSBYUSER.ordinal(), this.txn_id);

            client.commitTransaction();
            return true;
        } catch (DatabaseAbortException e) {
            return false;
        }
    }
}
