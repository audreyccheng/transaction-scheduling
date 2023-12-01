package shield.proxy.trx.concurrency;

import java.util.List;
import java.util.Random;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import shield.config.NodeConfiguration;
import shield.proxy.Proxy;
import shield.proxy.trx.concurrency.Transaction.TxState;
// import shield.proxy.trx.data.BatchDataHandler;
// import shield.proxy.trx.data.DataHandler;
// import shield.proxy.trx.data.SimpleDataHandler;

import org.rocksdb.*;
import static java.nio.charset.StandardCharsets.UTF_8;

public class RocksDBTransactionManager extends TransactionManager {

    private static final String dbPath = "/tmp/db";

    private Options options;
    private OptimisticTransactionDB txnDb;

    private TransactionDBOptions txnDbOptions;
    private TransactionDB txnDb2;

    private WriteOptions writeOptions;
    private ReadOptions readOptions;

    /**
    * Backpointer to configuration
    */
    private final NodeConfiguration config;

    private AtomicLong totalOperations = new AtomicLong();

    /**
    * Abort count statistics. Todo: fix
    */
    public int aborts = 0;

    public RocksDBTransactionManager(Proxy proxy) {
        super(proxy);
        this.config = proxy.getConfig();
        this.options = new Options().setCreateIfMissing(true);
        System.out.println("maxBackgroundJobs: " + this.options.softPendingCompactionBytesLimit());
        this.options.setMaxBackgroundJobs(8);
        this.options.setMaxSubcompactions(8);
        this.options.setAllowConcurrentMemtableWrite(true);
        this.options.setEnablePipelinedWrite(true);
        this.options.setRandomAccessMaxBufferSize(8589934592L);
        this.options.setDbWriteBufferSize(134217728L);
        this.options.setMaxWriteBufferNumber(5);
        // this.options.randomAccessMaxBufferSize(8589934592);
        // this.options.setWriteBufferSize(1000000);
        // this.options.setMaxWriteBufferNumberToMaintain(512).setMaxWriteBufferNumber(5);

        this.txnDbOptions = new TransactionDBOptions();

        if (this.config.DELETE_DB) {
            try {
                RocksDB.destroyDB(this.dbPath, this.options);
                System.out.println("Destroyed DB");
            } catch (RocksDBException err) {
                System.out.println("RocksDB delete DB");
            }
        }

         try { // final OptimisticTransactionDB txnDb
            if (!config.USE_PESSIMISTIC) {
                this.txnDb = OptimisticTransactionDB.open(this.options, this.dbPath);
                System.out.println("Created OptimisticTransactionDB");
            } else {
                this.txnDb2 = TransactionDB.open(this.options, this.txnDbOptions, this.dbPath);
                System.out.println("Created TransactionDB");
            }


        // try(final org.rocksdb.Options options = new Options()
        // .setCreateIfMissing(true);
        // final OptimisticTransactionDB txnDb =
        //     OptimisticTransactionDB.open(options, dbPath)) {

    //   try (final WriteOptions writeOptions = new WriteOptions();
    //        final ReadOptions readOptions = new ReadOptions()) {

        ////////////////////////////////////////////////////////
        //
        // Simple OptimisticTransaction Example ("Read Committed")
        //
        ////////////////////////////////////////////////////////
        // readCommitted(txnDb, writeOptions, readOptions);


        ////////////////////////////////////////////////////////
        //
        // "Repeatable Read" (Snapshot Isolation) Example
        //   -- Using a single Snapshot
        //
        ////////////////////////////////////////////////////////
        // repeatableRead(this.txnDb, writeOptions, readOptions);


        ////////////////////////////////////////////////////////
        //
        // "Read Committed" (Monotonic Atomic Views) Example
        //   --Using multiple Snapshots
        //
        ////////////////////////////////////////////////////////
        // readCommitted_monotonicAtomicViews(txnDb, writeOptions, readOptions);
    //   } catch (RocksDBException err) {
    //     System.out.println("RocksDB 3");
    //   }
    // } catch (RocksDBException err) {
    //   System.out.println("RocksDB 4");
    // }


         } catch (RocksDBException err) {
             System.out.println("Failed to open OptimisticTransactionDB");
         }

        System.out.println("Started RocksDBTransactionManager!");


    }

    private static void repeatableRead(final OptimisticTransactionDB txnDb,
      final WriteOptions writeOptions, final ReadOptions readOptions)
      throws RocksDBException {

    final byte key1[] = "ghi".getBytes(UTF_8);
    final byte value1[] = "jkl".getBytes(UTF_8);

    // Set a snapshot at start of transaction by setting setSnapshot(true)
    System.out.println("RocksDB B");
    try(final OptimisticTransactionOptions txnOptions =
            new OptimisticTransactionOptions().setSetSnapshot(true);
        final org.rocksdb.Transaction txn =
            txnDb.beginTransaction(writeOptions, txnOptions)) {
      System.out.println("RocksDB S");
      final Snapshot snapshot = txn.getSnapshot();

      // Write a key OUTSIDE of transaction
      txnDb.put(writeOptions, key1, value1);

      // Read a key using the snapshot.
      readOptions.setSnapshot(snapshot);
      System.out.println("RocksDB G");
      final byte[] value = txn.getForUpdate(readOptions, key1, true);
      assert (value == null);

      try {
        // Attempt to commit transaction
        System.out.println("RocksDB C");
        txn.commit();
        // throw new IllegalStateException();
      } catch(final RocksDBException e) {
        // Transaction could not commit since the write outside of the txn
        // conflicted with the read!
        System.out.println("RocksDB NC");
        assert(e.getStatus().getCode() == Status.Code.Busy);
      }

      txn.rollback();
      System.out.println("RocksDB D");
    } finally {
      // Clear snapshot from read options since it is no longer valid
      readOptions.setSnapshot(null);
    }
  }

    public void startTransaction(long clientId, Transaction t, int type) {
        boolean success = false;
        // System.out.println("startTransaction");
        this.writeOptions = new WriteOptions();
        this.readOptions = new ReadOptions();
        if (!config.USE_PESSIMISTIC) {
            final OptimisticTransactionOptions txnOptions =
                new OptimisticTransactionOptions().setSetSnapshot(true);
            org.rocksdb.Transaction txn = this.txnDb.beginTransaction(writeOptions, txnOptions);
    //        org.rocksdb.Transaction txn2 = txn;
            t.txnRDB = txn;
        } else {
            final TransactionOptions txnOptions = new TransactionOptions().setSetSnapshot(true);
            org.rocksdb.Transaction txn = this.txnDb2.beginTransaction(writeOptions, txnOptions);
            t.txnRDB = txn;
        }

    //    t.putSnapshot(t.txnRDB.getSnapshot());
        // t.txnRDB = this.txnDb.beginTransaction(writeOptions, txnOptions); //t.writeOptions, t.txnOptions);
        // System.out.println("RocksDB ST");

//        t.setSnapshot(txn.getSnapshot());
        // System.out.println("RocksDB SS");

//        t.txnRDB.getSnapshot();
//        System.out.println("RocksDB SS2");
        // try (final org.rocksdb.Transaction txn = this.txnDb.beginTransaction(t.writeOptions, t.txnOptions)) {
            //  t.setTxn(txn);
        // } catch (RocksDBException err) {
        //     System.out.println("Failed to start OptimisticTransaction");
        // }

        t.cluster = type; // TODO(accheng): update to predicting type
        // testTransactionStar ted(txn, success, t);

        proxy.executeAsync( () -> onTransactionStarted(t, success),
            proxy.getDefaultExpHandler());
    }

    // public void startTransaction(long clientId, Transaction t, int type) {
    //      boolean success = false;
    //     // System.out.println("startTransaction");
    //     final WriteOptions writeOptions = new WriteOptions();
    //     final ReadOptions readOptions = new ReadOptions();
    //     final TransactionOptions txnOptions = new TransactionOptions().setSetSnapshot(true);

    //     org.rocksdb.Transaction txn = this.txnDb.beginTransaction(writeOptions, txnOptions);
    //     t.txnRDB = txn;
    //     t.cluster = type;
    //     proxy.executeAsync( () -> onTransactionStarted(t, success),
    //         proxy.getDefaultExpHandler());
    // }

    // private void testTransactionStarted(org.rocksdb.Transaction trx, boolean success, Transaction t) {
    //     trx.getSnapshot();
    //     System.out.println("RocksDB SS");

    //     proxy.executeAsync( () -> onTransactionStarted(t, success),
    //         proxy.getDefaultExpHandler());
    // }

    public void onTransactionStarted(Transaction trx, boolean success) {
        // TODO(accheng): set Transaction t cluster
        boolean succ = success;
        System.out.println("onTransactionStarted cluster: " + trx.cluster);
       try {
           if (trx.cluster != 0) {
               trx.txnRDB.schedule(trx.cluster);
           }
           trx.txnRDB.setSnapshot();
           if (!config.USE_PESSIMISTIC) {
            trx.snapshot = this.txnDb.getSnapshot();
           } else {
            trx.snapshot = this.txnDb2.getSnapshot();
           }
           trx.readOptions.setSnapshot(trx.snapshot);
            // trx.putSnapshot(trx.txnRDB.getSnapshot()); // TODO(accheng): set before or after schedule?
            succ = true;
            //  System.out.println("TransactionScheduled");
       } catch (RocksDBException err) {
           System.out.println("Failed to schedule OptimisticTransaction");
       }
        final boolean finalSuccess = succ;
        proxy.executeAsync( () -> clientManager.onTransactionStarted(trx, finalSuccess),
            proxy.getDefaultExpHandler());
    }

    public void doRead(Long key, Operation op, Transaction trx) {
        final byte keyRDB [] = Long.toString(key).getBytes(UTF_8);
        try {
            byte[] value = trx.txnRDB.get(trx.readOptions, keyRDB); //txnDb.get(this.readOptions,keyRDB); //
            op.setReadValue(value);
        } catch (RocksDBException err) {
            op.markError();
            // System.out.println("Failed to read OptimisticTransaction");
        }
    }

    public void doReadForUpdate(Long key, Operation op, Transaction trx) {
        // System.out.println("[ReadFor] doReadForUpdate " + key);
        final byte keyRDB [] = Long.toString(key).getBytes(UTF_8);
        // trx.txnRDB.setSnapshotOnNextOperation();
        try {
            byte[] value = trx.txnRDB.getForUpdate(trx.readOptions, keyRDB, true /* fail commit for Optimistic */); //txnDb.get(this.readOptions,keyRDB); //
            op.setReadValue(value);
        } catch (RocksDBException err) {
            op.markError();
            // System.out.println("Failed to readForUpdate OptimisticTransaction");
        }
    }

    public void doWrite(Long key, Operation op, Transaction trx) {
        // System.out.println("doWrite " + key);
        final byte keyRDB [] = Long.toString(key).getBytes(UTF_8);
        // trx.txnRDB.setSnapshotOnNextOperation();
        try {
            trx.txnRDB.put(keyRDB, op.getWriteValue()); //txnDb.put(this.writeOptions,keyRDB,op.getWriteValue()); //
            // System.out.println("doWrite done");
        } catch (RocksDBException err) {
            op.markError();
            // System.out.println("Failed to write OptimisticTransaction");
        }
    }

    public void doDelete(Long key, Operation op, Transaction trx) {
        final byte keyRDB [] = Long.toString(key).getBytes(UTF_8);
        try {
            trx.txnRDB.delete(keyRDB);
        } catch (RocksDBException err) {
            op.markError();
            // System.out.println("Failed to delete OptimisticTransaction");
        }
    }

    public void doDummyWrite(Long key, Operation op, Transaction trx) {
        throw new RuntimeException("Unimplemented");
    }

    public void executeOperation(Operation op) {
        Transaction trx = op.getTrx();
        if (trx.getTrxState() == TxState.ABORTED) {
            // This transaction has already been aborted
        } else {
            if (config.LOG_TOTAL_OPERATIONS) {
            if (totalOperations.incrementAndGet()%500==0)
                config.statsTotalOperations.addPoint(totalOperations);
            }
            // System.out.println("executeOperation" + op.getType());
            switch (op.getType()) {

            case READ:
                doRead(op.getKey(), op, trx);
                break;
            case READ_FOR_UPDATE:
                // System.out.println("[ReadFor] Read for update!!!");
                doReadForUpdate(op.getKey(), op, trx);
                break;
            case WRITE:
                doWrite(op.getKey(), op, trx);
                break;
            case DELETE:
                doDelete(op.getKey(), op, trx);
                break;
            case DUMMY:
                doDummyWrite(op.getKey(), op, trx);
                break;

            }
        }
        onOperationExecuted(op);
    }

    public void onOperationExecuted(Operation op) {
        clientManager.onOperationExecuted(op);
    }

    public void commitTransaction(Transaction trx) {
        System.out.println("commitTransaction");
        assert (trx != null);
        try {
            // Attempt to commit transaction
            trx.txnRDB.commit();
            trx.clearSnapshot();
            trx.setTrxState(TxState.COMMITTED);
            onTransactionCommitted(trx);
        } catch(final RocksDBException e) {
            // System.out.println("commitTransaction abort with conflict: " + (e.getStatus().getCode() == Status.Code.Busy));
            // Rollback and abort if we cannot commit
            abortTransaction(trx);
        }
    }

    public void onTransactionCommitted(Transaction trx) {
        System.out.println("commitTransaction success cluster: " + trx.cluster);
        clientManager.onTransactionCommitted(trx);
    }

    public void abortTransaction(Transaction trx) {
        System.out.println("abortTransaction cluster: " + trx.cluster);
        trx.setTrxState(TxState.ABORTED);
        trx.clearSnapshot();

        try {
            trx.txnRDB.rollback();
        } catch (RocksDBException err) {
            System.out.println("Failed to rollback OptimisticTransaction");
        }

        onTransactionAborted(trx);
    }

    public void onTransactionAborted(Transaction trx) {
        clientManager.onTransactionAborted(trx);
    }

    public long getCurrentTs() {
        throw new RuntimeException("Unimplemented");
    }

    public void startTrxManager() {}
}
