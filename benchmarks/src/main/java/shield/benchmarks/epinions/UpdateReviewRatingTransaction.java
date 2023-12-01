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

public class UpdateReviewRatingTransaction extends BenchmarkTransaction {
    private EpinionsExperimentConfiguration config;
    private int iid;
    private int uid;
    private int rating;
    private long txn_id;

    public UpdateReviewRatingTransaction(EpinionsGenerator generator, int iid, int uid, int rating, long txn_id) {
        this.iid = iid;
        this.uid = uid;
        this.rating = rating;
        this.txn_id = txn_id;
        this.client = generator.getClient();
        this.config = generator.getConfig();
    }

    public static boolean isEmptyRow(byte[] row) {
        return row.length == 0;
    }

    public boolean tryRun() {
        try {
            List<byte[]> results;
            byte[] rowRating1;

            Table reviewTable = client.getTable(EpinionsConstants.kReviewTable);
            Table reviewByIIDTable = client.getTable(EpinionsConstants.kReviewByIIDTable);

            client.startTransaction();

            int type = this.uid + 101 + 20;
            if (this.uid > 19) {
                type = 0;
            }
            if (this.config.SCHEDULE && type != 0) {
                // System.out.println("Scheduling cluster: " + type);
                client.scheduleTransaction(type);
            }

            results = client.readAndExecute(EpinionsConstants.kReviewByIIDTable,
                    Integer.toString(iid)); //, EpinionsConstants.Transactions.UPDATEREVIEWRATING.ordinal(), this.txn_id);

            SerializableIDSet ids;
            if (results.size() > 0) {
                // this works because all three tables are structured identically
                byte[] nameRow = results.get(0);
                if (isEmptyRow(results.get(0))) nameRow = reviewByIIDTable.createNewRow(config.PAD_COLUMNS);
                ids = new shield.benchmarks.epinions.SerializableIDSet(config,
                        (String) reviewByIIDTable.getColumn("ID_LIST", nameRow));
            } else {
                ids = new SerializableIDSet(config);
            }
            if (results.size() > 1) {
                for (int i = 1; i < results.size(); i++){
                    byte[] nameRow = results.get(i);
                    if (isEmptyRow(results.get(i))) nameRow = reviewByIIDTable.createNewRow(config.PAD_COLUMNS);
                    ids.intersect(new shield.benchmarks.epinions.SerializableIDSet(config,
                            (String) reviewByIIDTable.getColumn("ID_LIST", nameRow)));
                }
            }

            List<Integer> idList = ids.toList();
            if (idList.size() != 0) {
                System.out.println("URI size: " + idList.size());
            }
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
                        } else {
                            rowRating1 = results.get(0);
                            break;
                        }
                    }
                }
            }

            if (results != null) {
                rowRating1 = results.get(0);
//                reviewTable.updateColumn("R_RATING", rating, rowRating1);


                client.writeAndExecute(EpinionsConstants.kReviewTable, Integer.toString(uid), rowRating1); //,
                        // EpinionsConstants.Transactions.UPDATEREVIEWRATING.ordinal(), this.txn_id);
            }

            client.commitTransaction();

            return true;

        } catch (DatabaseAbortException e) {
            return false;
        }
    }
}
