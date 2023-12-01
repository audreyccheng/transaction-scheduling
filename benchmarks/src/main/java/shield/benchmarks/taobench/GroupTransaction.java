package shield.benchmarks.taobench;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import shield.benchmarks.utils.BenchmarkTransaction;
import shield.benchmarks.utils.Generator;
import shield.client.DatabaseAbortException;
import shield.client.RedisPostgresClient;
import shield.client.schema.Table;

public class GroupTransaction extends BenchmarkTransaction {
    private TaoBenchGenerator generator;
    private int txnSize;
    private long txn_id;

    public GroupTransaction(TaoBenchGenerator generator, int txnSize, long txn_id) {
        this.txnSize= txnSize;
        this.client = generator.getClient();
        this.generator = generator;
        this.txn_id = txn_id;
    }

    @Override
    public boolean tryRun() {
        try {
            Set<Integer> keys = new HashSet<Integer>();
            List<byte[]> results;

            client.startTransaction();

            for (int i = 0; i < this.txnSize; i++) {
                Integer objIdRand = -1;
                while (objIdRand < 0) {
                    Integer key;
                    if (i % 2 == 0) { // //
                        key = generator.generateZipf(1); // + group2; // ;
                    } else {
                        key = Generator.generateInt(0, 100000); // ; //
                    }
                    if (!keys.contains(key)) {
                        objIdRand = key;
                        keys.add(key);
                    }
                }

//                int x = Generator.generateInt(0, 100);
                ((RedisPostgresClient) client).read(TaoBenchConstants.kObjectsTable, objIdRand.toString(),
                        TaoBenchConstants.Transactions.READ.ordinal(), this.txn_id);
            }
            results = client.execute();

            client.commitTransaction();
            return true;

        } catch (DatabaseAbortException e) {
            return false;
        }
    }
}
