package shield.benchmarks.tpcc;

import java.util.List;
import shield.benchmarks.tpcc.utils.TPCCConstants;
import shield.benchmarks.utils.BenchmarkTransaction;
import shield.client.DatabaseAbortException;
import shield.client.RedisPostgresClient;
import shield.client.schema.Table;
import shield.benchmarks.utils.Generator;

public class TPaymentTransaction extends BenchmarkTransaction{

  private int cwid;

  private int cdid;

  private int cid;

  private int wid;

  private int did;

  private int amount;

  private boolean accessByLastName;

  private String customerName;

  private TPCCExperimentConfiguration config;

  private TPCGenerator generator;
  private long txn_id;

  public TPaymentTransaction(TPCGenerator generator,
      int cwid, int cdid, int cid, int wid, int did, int amount,
      boolean accessByLastName, String customerName, long txn_id) {
    this.cwid = cwid;
    this.cdid = cdid;
    this.cid = cid;
    this.wid = wid;
    this.did = did;
    this.amount = amount;
    this.accessByLastName = accessByLastName;
    this.customerName = customerName;
    this.generator = generator;
    this.client = generator.getClient();
    this.config = generator.getConfig();
    this.txn_id = txn_id;
  }

  public boolean tryRun() {
    try {
      List<byte[]> results;
      byte[] result;
      WarehouseKey wKey;
      String wName;
      DistrictKey dKey;
      String dName;
      CustomerKey cKey;
      HistoryKey hKey;
      CustomerByNameKey cByNameKey;
      String hData;
      String now;
      int dYtd;
      int wYtd;
      int cBalance;
      int cPaymentCnt;
      int cYtd;

      Table warehouseTable  = client.getTable(TPCCConstants.kWarehouseTable);
      Table districtTable = client.getTable(TPCCConstants.kDistrictTable);
      Table customerTable = client.getTable(TPCCConstants.kCustomerTable);
      Table historyTable = client.getTable(TPCCConstants.kHistoryTable);
      Table customerByNameTable = client.getTable(TPCCConstants.kCustomerByNameTable);

      client.startTransaction();
      // System.out.println("[Payment]");
      if (this.config.SCHEDULE) {
        int type = wid + 1 + 100;
        // System.out.println("Scheduling P cluster:" + type);
        client.scheduleTransaction(type);
      } else if (this.config.DEFER) {
        int key_set_size = 3;
        double lookup_prob = (2*1.0)/key_set_size;
        double defer_prob = 0.60;
        int defer = (int) ((lookup_prob * defer_prob) * 100);
        // System.out.println("Scheduling NO defer:" + defer);
        if (Generator.generateInt(0,100) < defer) {
          int type = wid + 1 + 100;

          client.scheduleTransaction(type);
        }
      }

      wKey = new WarehouseKey(wid);
      //TODO(add readForUpdate)
      //  client.readForUpdateAndExecute(TPCCConstants.kWarehouseTable, wKey.str());
      client.readForUpdate(TPCCConstants.kWarehouseTable, wKey.str());
      dKey = new DistrictKey(wid, did);
      if (!accessByLastName) {
        results = client.readForUpdateAndExecute(TPCCConstants.kDistrictTable, dKey.str());
      } else {
        assert(false);
        // client.readForUpdateAndExecute(TPCCConstants.kDistrictTable, dKey.str());
        client.readForUpdate(TPCCConstants.kDistrictTable, dKey.str());
        // Add an additional scan to the customerbyNname table
        cByNameKey = new CustomerByNameKey(wid,did,customerName);
        results = client.readAndExecute(TPCCConstants.kCustomerByNameTable, cByNameKey.str());

      }
      // System.out.println("[Payment] " + wid + " " +  did);
      result = results.get(0);
      wName = (String) warehouseTable.getColumn(1, result, wKey.value());
      wYtd = (Integer) warehouseTable.getColumn(8, result);
      //TODO(natacha): need to also retrieve W_STREET_1, W_STREET_2, W_CITY,W_STATE,W_ZIP
      warehouseTable.updateColumn(8, wYtd + amount, result);
      // client.updateAndExecute(TPCCConstants.kWarehouseTable,wKey.str(), result);
      client.update(TPCCConstants.kWarehouseTable,wKey.str(), result);
      result = results.get(0);
      dName = (String) districtTable.getColumn(2, result);
      dYtd = (Integer) districtTable.getColumn(9, result);
      //TODO(natacha): need to also retrieve the aforementioned fields
      districtTable.updateColumn(9, dYtd + amount, result);
      // System.out.println("[Payment District key] " + wid + " " +  did + " " + dKey.str());
      // client.updateAndExecute(TPCCConstants.kDistrictTable,dKey.str(), result);
      client.update(TPCCConstants.kDistrictTable,dKey.str(), result);

      if (accessByLastName) {
        result = results.get(0);
        String customers = (String) customerByNameTable.getColumn(0, result);
        int nbCustomers = generator.getNbElements(customers);
        // Use the id of the n/2 customer (as specified in clause)
        cid = Integer.parseInt(generator.getElementAtIndex(customers,nbCustomers/2));
      }

      // System.out.println("[Payment] wid:" + wid + " did:" +  did + " cid:" + cid);

      cKey = new CustomerKey(cwid,cdid,cid);
      this.generator.updateCustomer(cKey.str());
      results = client.readForUpdateAndExecute(TPCCConstants.kCustomerTable, cKey.str());
       result = results.get(0);
      //TODO(natacha): explicitly retrieve C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY,
      // C_STATE, C_ZIP, C_PH ON E, C_SIN CE, C_CREDIT, C_CREDIT_LIM, C_DISCOUN T
      // TODO(natacha): add the C_CREDIT/BC check
      cBalance = (Integer) customerTable.getColumn(16, result);
      cYtd = (Integer) customerTable.getColumn(17,result);
      cPaymentCnt = (Integer) customerTable.getColumn(18, result);
      customerTable.updateColumn(16,cBalance-amount, result);
      customerTable.updateColumn(17, cYtd + amount, result);
      customerTable.updateColumn(18,cPaymentCnt + 1, result);
      // client.updateAndExecute(TPCCConstants.kCustomerTable, cKey.str(), result);
      client.update(TPCCConstants.kCustomerTable, cKey.str(), result);

      hKey = new HistoryKey(generator.GetNextHistoryKey());
      hData = wName + dName;
      now = generator.getTime();
      result = historyTable.createNewRow(config.PAD_COLUMNS);
      historyTable.updateColumn(0,cid,result);
      historyTable.updateColumn(1,cdid,result);
      historyTable.updateColumn(2,cwid,result);
      historyTable.updateColumn(3,did,result);
      historyTable.updateColumn(4,wid,result);
      historyTable.updateColumn(5,now,result);
      historyTable.updateColumn(6, amount, result);
      historyTable.updateColumn(7,hData,result);
      // client.writeAndExecute(TPCCConstants.kHistoryTable,hKey.str(),result);
      client.write(TPCCConstants.kHistoryTable,hKey.str(),result);

      client.commitTransaction();
      return true;
    } catch (DatabaseAbortException e) {

    }
    return false;
  }
}
