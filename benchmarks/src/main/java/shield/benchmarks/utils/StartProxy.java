package shield.benchmarks.utils;

import java.security.NoSuchAlgorithmException;
import org.json.simple.parser.ParseException;
import shield.client.DatabaseAbortException;
import shield.proxy.Proxy;
import org.rocksdb.*;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;

/**
 * Class starts a proxy, based on the current configuration file
 *
 * @author ncrooks
 */
public class StartProxy {
  private static final String dbPath = "/tmp/db";

  public static void main(String[] args) throws InterruptedException,
      IOException, ParseException, DatabaseAbortException, NoSuchAlgorithmException {
        RocksDB.loadLibrary();

        try(final Options options = new Options()
        .setCreateIfMissing(true);
        final OptimisticTransactionDB txnDb =
            OptimisticTransactionDB.open(options, dbPath)) {

      try (final WriteOptions writeOptions = new WriteOptions();
           final ReadOptions readOptions = new ReadOptions()) {

        ////////////////////////////////////////////////////////
        //
        // Simple OptimisticTransaction Example ("Read Committed")
        //
        ////////////////////////////////////////////////////////
        readCommitted(txnDb, writeOptions, readOptions);


        ////////////////////////////////////////////////////////
        //
        // "Repeatable Read" (Snapshot Isolation) Example
        //   -- Using a single Snapshot
        //
        ////////////////////////////////////////////////////////
        // repeatableRead(txnDb, writeOptions, readOptions);


        ////////////////////////////////////////////////////////
        //
        // "Read Committed" (Monotonic Atomic Views) Example
        //   --Using multiple Snapshots
        //
        ////////////////////////////////////////////////////////
        // readCommitted_monotonicAtomicViews(txnDb, writeOptions, readOptions);
      } catch (RocksDBException err) {
        System.out.println("RocksDB 3");
      }
    } catch (RocksDBException err) {
      System.out.println("RocksDB 4");
    }


    String expConfigFile;

    if (args.length != 1) {
      System.err.println(
          "Incorrect number of arguments: expected <expConfigFile.json>");
    }
    // Contains the experiment paramaters
    expConfigFile = args[0];

    Proxy proxy = new Proxy(expConfigFile);
    proxy.startProxy();
  }

  private static void readCommitted(final OptimisticTransactionDB txnDb,
      final WriteOptions writeOptions, final ReadOptions readOptions)
      throws RocksDBException {
    final byte key1[] = "abc".getBytes(UTF_8);
    final byte value1[] = "def".getBytes(UTF_8);

    final byte key2[] = "xyz".getBytes(UTF_8);
    final byte value2[] = "zzz".getBytes(UTF_8);

    // Start a transaction
    try(final Transaction txn = txnDb.beginTransaction(writeOptions)) {
      // Read a key in this transaction
      byte[] value = txn.get(readOptions, key1);
      assert(value == null);

      // Write a key in this transaction
      txn.put(key1, value1);

      // Read a key OUTSIDE this transaction. Does not affect txn.
      value = txnDb.get(readOptions, key1);
      assert(value == null);

      // Write a key OUTSIDE of this transaction.
      // Does not affect txn since this is an unrelated key.
      // If we wrote key 'abc' here, the transaction would fail to commit.
      txnDb.put(writeOptions, key2, value2);

      // Commit transaction
      txn.commit();
      System.out.println("RocksDB RC C");
    }
  }

  private static void readCommitted_monotonicAtomicViews(
      final OptimisticTransactionDB txnDb, final WriteOptions writeOptions,
      final ReadOptions readOptions) throws RocksDBException {

    final byte keyX[] = "x".getBytes(UTF_8);
    final byte valueX[] = "x".getBytes(UTF_8);

    final byte keyY[] = "y".getBytes(UTF_8);
    final byte valueY[] = "y".getBytes(UTF_8);

    try (final OptimisticTransactionOptions txnOptions =
             new OptimisticTransactionOptions().setSetSnapshot(true);
         final Transaction txn =
             txnDb.beginTransaction(writeOptions, txnOptions)) {

      // Do some reads and writes to key "x"
      Snapshot snapshot = txnDb.getSnapshot();
      readOptions.setSnapshot(snapshot);
      byte[] value = txn.get(readOptions, keyX);
      txn.put(valueX, valueX);

      // Do a write outside of the transaction to key "y"
      // txnDb.put(writeOptions, keyY, valueY);


      // Write a key OUTSIDE of transaction
      final WriteOptions writeOptions2 = new WriteOptions();
      final OptimisticTransactionOptions txnOptions2 =
            new OptimisticTransactionOptions().setSetSnapshot(true);
      final Transaction txn2 =
            txnDb.beginTransaction(writeOptions2, txnOptions2);
      final ReadOptions readOptions2 = new ReadOptions();
      txn2.put(keyY, valueY);
      try {
        txn2.commit();
        System.out.println("RocksDB C 2");
      } catch(final RocksDBException e) {
        System.out.println("RocksDB NC 2");
      }

      // Set a new snapshot in the transaction
      txn.setSnapshot();
      snapshot = txnDb.getSnapshot();
      readOptions.setSnapshot(snapshot);

      // Do some reads and writes to key "y"
      // Since the snapshot was advanced, the write done outside of the
      // transaction does not conflict.
      value = txn.getForUpdate(readOptions, keyY, true);
      txn.put(keyY, valueY);

      // Commit.  Since the snapshot was advanced, the write done outside of the
      // transaction does not prevent this transaction from Committing.
      txn.commit();
      System.out.println("RocksDB C");
    } finally {
      // Clear snapshot from read options since it is no longer valid
      readOptions.setSnapshot(null);
    }
  }

  private static void repeatableRead(final OptimisticTransactionDB txnDb,
      final WriteOptions writeOptions, final ReadOptions readOptions)
      throws RocksDBException {

    final byte key1[] = "ghi".getBytes(UTF_8);
    final byte value1[] = "jkl".getBytes(UTF_8);
    final byte value2[] = "mno".getBytes(UTF_8);

    // Set a snapshot at start of transaction by setting setSnapshot(true)
    System.out.println("RocksDB B");
    try(final OptimisticTransactionOptions txnOptions =
            new OptimisticTransactionOptions().setSetSnapshot(true);
        final Transaction txn =
            txnDb.beginTransaction(writeOptions, txnOptions)) {
      System.out.println("RocksDB S");
      // txn.setSnapshot();
      txn.setSnapshotOnNextOperation();

      txn.setSnapshot();
      Snapshot snapshot = txnDb.getSnapshot();
      // Read a key using the snapshot.
      readOptions.setSnapshot(snapshot);
      // System.out.println("RocksDB G");
      // final byte[] value = txn.get(readOptions, key1); // txn.getForUpdate(readOptions, key1, true);
      // assert (value == null);


      // Write a key OUTSIDE of transaction
      final WriteOptions writeOptions2 = new WriteOptions();
      final OptimisticTransactionOptions txnOptions2 =
            new OptimisticTransactionOptions().setSetSnapshot(true);
      final Transaction txn2 =
            txnDb.beginTransaction(writeOptions2, txnOptions2);
      final ReadOptions readOptions2 = new ReadOptions();



      // readOptions2.setSnapshot(null);
      txn2.setSnapshot();
      readOptions2.setSnapshot(txnDb.getSnapshot());

      // final byte[] valueN = txn2.getForUpdate(readOptions2, key1, true);
      // assert (valueN != null);

      byte[] v = txn2.get(readOptions2, key1);
      // txn2.put(key1, value2);
      try {
        txn2.commit();
        System.out.println("RocksDB C 2");
      } catch(final RocksDBException e) {
        System.out.println("RocksDB NC 2");
      }


      txn.put(key1, value1);

      try {
        // Attempt to commit transaction
        txn.commit();
        System.out.println("RocksDB C");
        // throw new IllegalStateException();
      } catch(final RocksDBException e) {
        // Transaction could not commit since the write outside of the txn
        // conflicted with the read!
        System.out.println("RocksDB NC");
        assert(e.getStatus().getCode() == Status.Code.Busy);
      }

      // txn.rollback();
      // System.out.println("RocksDB D");
    } finally {
      // Clear snapshot from read options since it is no longer valid
      readOptions.setSnapshot(null);
    }
  }

}
