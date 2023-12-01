package shield.benchmarks.tpcc;

import java.util.List;
import shield.benchmarks.tpcc.utils.TPCCConstants;
import shield.benchmarks.utils.BenchmarkTransaction;
import shield.benchmarks.utils.Generator;
import shield.client.DatabaseAbortException;
import shield.client.Client;
import shield.client.schema.Table;

public class TOrderStatusTransaction extends BenchmarkTransaction{

  private int wid;

  private int cid;

  private int did;

  private boolean accessByLastName;

  private String customerName;

  private TPCCExperimentConfiguration config;

  private TPCGenerator generator;

  public TOrderStatusTransaction(TPCGenerator generator,
      int wid, int cid, int did, boolean accessByLastName, String customerName) {
    this.wid = wid;
    this.cid = cid;
    this.did = did;
    this.accessByLastName = accessByLastName;
    this.customerName = customerName;
    this.generator = generator;
    this.config = generator.getConfig();
    this.client = generator.getClient();
  }


  @Override
  public boolean tryRun() {
    try {
      List<byte[]> results;
      OrderByCustomerKey obcKey;
      byte[] obcVal;
      int oid;
      OrderKey oKey;
      byte[] orderVal;
      int olCnt = 15;
      OrderLineKey olKey;
      byte[] orderLineVal;
      CustomerKey cKey;
      byte[] customerVal;
      CustomerByNameKey cByNameKey;

      Table orderByCustTable = client.getTable(TPCCConstants.kOrderByCustomerTable);
      Table orderTable = client.getTable(TPCCConstants.kOrderTable);
      Table orderLineTable = client.getTable(TPCCConstants.kOrderLineTable);
      Table custTable = client.getTable(TPCCConstants.kCustomerTable);
      Table custByNameTable = client.getTable(TPCCConstants.kCustomerByNameTable);

      client.startTransaction();
      // System.out.println("[OS]");
      if (accessByLastName) {
        cByNameKey = new CustomerByNameKey(wid, did, customerName);
        results = client.readAndExecute(TPCCConstants.kCustomerByNameTable, cByNameKey.str());
        customerVal = results.get(0);
        String customers = (String) custByNameTable.getColumn(0, customerVal);
        int nbCustomers = generator.getNbElements(customers);
        // Use the id of the n/2 customer (as specified in clause)
        cid = Integer.parseInt(generator.getElementAtIndex(customers, nbCustomers / 2));
      }
      // TRICK: use <W_ID, D_ID, C_ID> to form a "secondary index table"
      // It stores the most recent order from each user

      obcKey = new OrderByCustomerKey(wid,did,cid);
      results = client.readAndExecute(TPCCConstants.kOrderByCustomerTable, obcKey.str());
      obcVal = results.get(0);

      if (obcVal.length == 0) {
        // System.out.println("No order for this customer");
        client.commitTransaction();
        return true;
      }

      // Get Order Count
      oid =(Integer)  orderByCustTable.getColumn(0,obcVal);
      oKey = new OrderKey(wid,did,oid);
      results = client.readAndExecute(TPCCConstants.kOrderTable, oKey.str());
      // Check Order Line
      for (int i = 0 ; i <olCnt ; i++) {
        olKey = new OrderLineKey(wid,did,oid,i);
        // client.readAndExecute(TPCCConstants.kOrderLineTable, olKey.str());
        client.read(TPCCConstants.kOrderLineTable, olKey.str());
      }



      // We reorder the access to customer table to the end
      // Delivery transaction has a data dependency of "Order->OrderLine->Customer",
      // and it reads/writes all these three tables.
      // If we put access to customer table at the beginning in order status,
      // we may encounter deadlock.
      cKey = new CustomerKey(wid,did,cid);
      // client.readAndExecute(TPCCConstants.kCustomerTable, cKey.str());
      client.read(TPCCConstants.kCustomerTable, cKey.str());
      client.commitTransaction();

      orderVal = results.get(0);
      olCnt = (Integer) orderTable.getColumn(6,orderVal);
      // System.out.println("Order Count " + olCnt);
      return true;
    } catch (DatabaseAbortException e) {
      return false;
    }
  }
}
