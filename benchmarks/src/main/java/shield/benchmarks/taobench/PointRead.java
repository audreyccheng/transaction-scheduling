package shield.benchmarks.taobench;

import java.util.List;
import shield.benchmarks.utils.BenchmarkTransaction;
import shield.benchmarks.utils.Generator;
import shield.client.DatabaseAbortException;
import shield.client.RedisPostgresClient;
import shield.client.schema.Table;

public class PointRead extends BenchmarkTransaction {
    private Integer objId;
    private Integer objId2;
    private Boolean edge = false;
    private long txn_id;


    public PointRead(TaoBenchGenerator generator, int objId, long txn_id) {
        this.objId = objId;
        int x = Generator.generateInt(0,100);
        if (x < 0.5) {
            this.edge = true;
        }
        this.client = generator.getClient();
        this.txn_id = txn_id;
    }

    @Override
    public boolean tryRun() {
        try {

//            System.out.println("Point Read");

            List<byte[]> results;

            client.startTransaction();
            ((RedisPostgresClient) client).readAndExecute(TaoBenchConstants.kObjectsTable, this.objId.toString(),
                    TaoBenchConstants.Transactions.READ.ordinal(), this.txn_id);

            client.commitTransaction();
            return true;
        } catch (DatabaseAbortException e) {
            return false;
        }
    }
}
