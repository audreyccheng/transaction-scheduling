package shield.benchmarks.tpcc;

import java.util.List;
import java.util.Random;
import shield.benchmarks.tpcc.utils.TPCCConstants;
import shield.benchmarks.utils.BenchmarkTransaction;
import shield.benchmarks.utils.Generator;
import shield.client.DatabaseAbortException;
import shield.client.Client;
import shield.client.schema.Table;

public class TDeliveryTransaction extends BenchmarkTransaction {

  private int wid;

  private int carrierId;

  private int did;

  private TPCGenerator generator;
  private TPCCExperimentConfiguration config;

  private Random random;

  public TDeliveryTransaction(TPCGenerator generator,
       int wid, int carrierId, int did) {
    this.client = generator.getClient();
    this.config = generator.getConfig();
    this.generator = generator;
    this.wid = wid;
    this.carrierId = carrierId;
    this.did = did;
    this.generator = generator;
    this.config = generator.getConfig();
    this.client = generator.getClient();
    this.random = new Random();
  }

  @Override
  public int run() {
    int nbAborts = 0;
    int time = 10;
    double backOff = 5;
    double maxBackoff = 5000;
    for (did = 0; did < config.NB_DISTRICTS - 1; did++) {
      boolean succeed = false;
      while (!succeed) {
        succeed = tryRun();
        // if (!succeed) nbAborts++;
        if (!succeed) {
          // System.out.println("Aborting!");
          nbAborts++;

          // Backoff
          if (true) { // config.USE_BACKOFF) {
            try {
              backOff = (int) (backOff * 2 * (1.0 + random.nextDouble()));
              // backOff = Math.min(Generator.generateInt(0, (int) Math.pow(2, nbAborts) + 1), maxBackoff);
              // System.out.println("Backing Off " + backOff + " nbAborts: " + nbAborts);
              Thread.sleep((int) backOff);
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
          }
        }
      }
    }
    return nbAborts;
  }

  public boolean tryRun() {
    try {

      List<byte[]> results;
      byte[] earliestNo;
      byte[] no;
      EarliestNewOrderKey enoKey;
      NewOrderKey noKey;
      Integer noId;
      OrderKey oKey;
      byte[] order;
      Integer cid;
      Integer olCnt;
      byte[] olRow;
      CustomerKey cKey;
      byte[] custRow;
      Integer delCount;
      OrderLineKey olKey;
      int total = 0;
      String date;

      Table earliestNewOrderTable = client.getTable(TPCCConstants.kEarliestNewOrderTable);
      Table newOrderTable = client.getTable(TPCCConstants.kNewOrderTable);
      Table orderTable = client.getTable(TPCCConstants.kOrderTable);
      Table orderLineTable = client.getTable(TPCCConstants.kOrderLineTable);
      Table customerTable = client.getTable(TPCCConstants.kCustomerTable);

      client.startTransaction();
      // System.out.println("[D]");
      // Read earliest table to find the order to deliver
      enoKey =
          new EarliestNewOrderKey(wid, did);


      results = client.readAndExecute(TPCCConstants.kEarliestNewOrderTable, enoKey.str());
      earliestNo = results.get(0);
      if (earliestNo.length == 0) {
        // System.out.println("[Delivery] No earliest order");
        client.commitTransaction();
        return true;
      }

      noId = (Integer) earliestNewOrderTable.getColumn(2, earliestNo);

      noKey = new NewOrderKey(wid, did, noId);
      results = client.readAndExecute(TPCCConstants.kNewOrderTable, noKey.str());
      no = results.get(0);

      if (no.length == 0) {
        // if it is not ordered, stop
        // CHECK: are we still counting this as part of a successful delivery?
        // System.out.println("Item is not ordered");
        client.commitTransaction();
        return true;
      }

      // Update the earliest new order table
      noId = noId + 1;
      // System.out.println("[Delivery] Earliest New Order is Now " + wid + " " + did + " " + noId);
      earliestNewOrderTable.updateColumn(2, noId, no);
      // client.updateAndExecute(TPCCConstants.kEarliestNewOrderTable, enoKey.str(), no);
      client.update(TPCCConstants.kEarliestNewOrderTable, enoKey.str(), no);

      // Delete a record (for now, write the empty value)
      // client.deleteAndExecute(TPCCConstants.kNewOrderTable, noKey.str());
      client.delete(TPCCConstants.kNewOrderTable, noKey.str());

      // Read and update order
      oKey = new OrderKey(wid, did, noId);
      results = client.readForUpdateAndExecute(TPCCConstants.kOrderTable,
          oKey.str());
      order = results.get(0);
      cid = (Integer) orderTable.getColumn(3, order);
      olCnt = (Integer) orderTable.getColumn(6, order);

      assert (olCnt>= 5 && olCnt <=15 );

      // Update Carrier id
      orderTable.updateColumn(5, carrierId, order);
      // client.updateAndExecute(TPCCConstants.kOrderTable, oKey.str(), order);
      client.update(TPCCConstants.kOrderTable, oKey.str(), order);

      for (int i = 0; i < olCnt; i++) {
        olKey = new OrderLineKey(wid, did, noId, i);
        // Compute amount
        results = client.readForUpdateAndExecute(TPCCConstants.kOrderLineTable, olKey.str());
        olRow = results.get(0);
        total += (Integer) orderLineTable.getColumn(8, olRow);
        // Now update the delivery to the current time
        date = generator.getTime();
        orderLineTable.updateColumn(6, date, olRow);
        // client.updateAndExecute(TPCCConstants.kOrderLineTable, olKey.str(), olRow);
        client.update(TPCCConstants.kOrderLineTable, olKey.str(), olRow);
      }

      // Read and update customer table
      cKey = new CustomerKey(wid, did, cid);
      this.generator.updateCustomer(cKey.str());
      results = client.readForUpdateAndExecute(TPCCConstants.kCustomerTable, cKey.str());
     // results = client.readAndExecute(TPCCConstants.kCustomerTable, cKey.str());
      custRow = results.get(0);
      delCount = (Integer) customerTable.getColumn(19, custRow);
      customerTable.updateColumn(16, total, custRow);
      customerTable.updateColumn(19, 1 + delCount, custRow);
      // client.updateAndExecute(TPCCConstants.kCustomerTable, cKey.str(), custRow);
      client.update(TPCCConstants.kCustomerTable, cKey.str(), custRow);
      client.commitTransaction();
      return true;
    } catch (DatabaseAbortException e) {
      // System.out.println("Returning False");
      return false;
    } catch (Exception e) {
        try {
          client.abortTransaction();
        } catch (Exception ee) {
          System.exit(-1);
        }
    }
    return false;
  }
}
