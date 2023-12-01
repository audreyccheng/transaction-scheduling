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

public class ReviewItemByIDTransaction extends BenchmarkTransaction {
    private EpinionsExperimentConfiguration config;
    private int iid;
    private long txn_id;

    public ReviewItemByIDTransaction(EpinionsGenerator generator, int iid, long txn_id) {
        this.iid = iid;
        this.txn_id = txn_id;
        this.client = generator.getClient();
        this.config = generator.getConfig();
    }

    public static boolean isEmptyRow(byte[] row) {
        return row.length == 0;
    }

    public boolean tryRun() {
        try {

//            System.out.println("ReviewItemByIDTransaction");
            List<byte[]> results;

            Table reviewTable = client.getTable(EpinionsConstants.kReviewTable);
            Table reviewByIIDTable = client.getTable(EpinionsConstants.kReviewByIIDTable);
            Table itemTable = client.getTable(EpinionsConstants.kItemTable);

            client.startTransaction();

            results = client.readAndExecute(EpinionsConstants.kReviewByIIDTable,
                    Integer.toString(iid)); //, EpinionsConstants.Transactions.REVIEWITEMBYID.ordinal(), this.txn_id);

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
                System.out.println("RI size: " + idList.size());
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
                        }
                    }
                    System.out.println("ReviewItemByIDTransaction " + results.size());
                }
            }

            client.readAndExecute(EpinionsConstants.kItemTable, Integer.toString(iid)); //,
                    // EpinionsConstants.Transactions.REVIEWITEMBYID.ordinal(), this.txn_id);

            client.commitTransaction();
            return true;
        } catch (DatabaseAbortException e) {
            return false;
        }
    }
}
