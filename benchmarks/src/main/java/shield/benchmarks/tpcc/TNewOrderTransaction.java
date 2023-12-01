package shield.benchmarks.tpcc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import javax.xml.crypto.Data;
import shield.benchmarks.tpcc.utils.TPCCConstants;
import shield.benchmarks.utils.BenchmarkTransaction;
// import shield.client.RedisPostgresClient;
import shield.client.DatabaseAbortException;
import shield.client.schema.Table;
import shield.benchmarks.utils.Generator;

public class TNewOrderTransaction extends BenchmarkTransaction{

  private final TPCGenerator generator;
  private final TPCCExperimentConfiguration config;

  private final int wid;

  private final int did;

  private final int cid;

  private final int olCnt;

  private final Set<Integer> iids;

  private final List<Integer> swids;

  private final List<Integer> olQuantities;
  private long txn_id;

  public TNewOrderTransaction(TPCGenerator generator, int wid, int did, int cid, int olCnt,
      Set<Integer> iids, List<Integer> swids, List<Integer> olQuantities, long txn_id) {
    this.wid = wid;
    this.did = did;
    this.cid = cid;
    this.olCnt = olCnt;
    this.iids = iids;
    this.swids = swids;
    this.olQuantities = olQuantities;
    this.generator = generator;
    this.config = generator.getConfig();
    this.client = generator.getClient();
    this.txn_id = txn_id;
  }

  public boolean tryRun() {
    try {

      WarehouseKey wKey;
      DistrictKey dKey;
      CustomerKey cKey;
      NewOrderKey newOrderKey;
      OrderByCustomerKey orderByCustomerKey;
      ItemKey itemKey;
      OrderKey orderKey;
      OrderLineKey orderLineKey;
      StockKey stockKey;

      String wName;
      String dName;
      int wTax;
      int dTax;
      int dNextOid;
      List<byte[]> results;
      byte[] wRow;
      byte[] dRow;
      byte[] cRow;
      byte[] oRow;
      int cDiscount;
      String cLastName;
      String cCredit;
      int itemId;
      int olQuantity;
      int supplyWid;
      int price;
      boolean allLocal;
      int sQuantity;
      String distInfo;
      int stock;
      int amount = 0;
      String sData;
      String iData;
      int sYtd;
      int remoteCount;

      allLocal = true;
      for (int i = 0; i < olCnt; ++i) {
        if (swids.get(i) != wid) {
          allLocal = false;
          break;
        }
      }

      Table warehouseTable = client.getTable(TPCCConstants.kWarehouseTable);
      Table districtTable = client.getTable(TPCCConstants.kDistrictTable);
      Table customerTable = client.getTable(TPCCConstants.kCustomerTable);
      Table orderTable = client.getTable(TPCCConstants.kOrderTable);
      Table newOrderTable = client.getTable(TPCCConstants.kNewOrderTable);
      Table orderByCustTable = client.getTable(TPCCConstants.kOrderByCustomerTable);
      Table itemTable = client.getTable(TPCCConstants.kItemTable);
      Table stockTable = client.getTable(TPCCConstants.kStockTable);
      Table orderLineTable = client.getTable(TPCCConstants.kOrderLineTable);

      client.startTransaction();
      // System.out.println("[NewOrder]");
      if (this.config.SCHEDULE) {
        int type = wid * 10 + (did + 1);
        // System.out.println("Scheduling NO cluster:" + type);
        client.scheduleTransaction(type);
      }
      // else if (this.config.DEFER) {
      //   int key_set_size = 5;
      //   double lookup_prob = 2*1.0/key_set_size;
      //   double defer_prob = 0.60;
      //   int defer = (int) ((lookup_prob * defer_prob) * 100);
      //   // System.out.println("Scheduling NO defer:" + defer);
      //   if (Generator.generateInt(0,100) < defer) {
      //     int type = wid * 10 + (did + 1);
      //     // System.out.println("Scheduling NO cluster:" + type);
      //     client.scheduleTransaction(type);
      //   }
      // }

      wKey = new WarehouseKey(wid);
      dKey = new DistrictKey(wid, did);
      cKey = new CustomerKey(wid, did, cid);
      this.generator.updateCustomer(cKey.str());
      // results = client.readAndExecute(TPCCConstants.kWarehouseTable, wKey.str());
      // wRow = results.get(0);
      // results = client.readForUpdateAndExecute(TPCCConstants.kDistrictTable, dKey.str());
      // dRow = results.get(0);
      // results = client.readAndExecute(TPCCConstants.kCustomerTable, cKey.str());
      // cRow = results.get(0);

      client.read(TPCCConstants.kWarehouseTable, wKey.str());
      client.readForUpdate(TPCCConstants.kDistrictTable, dKey.str());
      results = client.readAndExecute(TPCCConstants.kCustomerTable, cKey.str());
      wRow = results.get(0);
      dRow = results.get(1);
      cRow = results.get(2);

      wTax = (Integer) warehouseTable.getColumn(7, wRow);
      dTax = (Integer) districtTable.getColumn(8, dRow);
      dNextOid = (Integer) districtTable.getColumn(10, dRow);
      // System.out.println("[NewOrder] Wid: " + wid + " " + did + " " + dNextOid);

      cDiscount = (Integer) customerTable.getColumn(15, cRow);
      cLastName = (String) customerTable.getColumn(5, cRow);
      cCredit = (String) customerTable.getColumn(13, cRow);

      districtTable.updateColumn(10, dNextOid + 1, dRow);
      client.updateAndExecute(TPCCConstants.kDistrictTable, dKey.str(), dRow);

      // Write new order

      newOrderKey = new NewOrderKey(wid, did, dNextOid);
      oRow = newOrderTable.createNewRow(config.PAD_COLUMNS);
      newOrderTable.updateColumn(0, dNextOid, oRow);
      newOrderTable.updateColumn(1, did, oRow);
      newOrderTable.updateColumn(2, wid, oRow);
      client.writeAndExecute(TPCCConstants.kNewOrderTable, newOrderKey.str(), oRow);

      // Write order - update secondary index
      oRow = orderByCustTable.createNewRow(config.PAD_COLUMNS);
      orderByCustomerKey = new OrderByCustomerKey(wid, did, cid);
      orderByCustTable.updateColumn(0, dNextOid, oRow);
      client.writeAndExecute(TPCCConstants.kOrderByCustomerTable, orderByCustomerKey.str(),
          oRow);

      // Write order
      oRow = orderTable.createNewRow(config.PAD_COLUMNS);
      orderKey = new OrderKey(wid, did, dNextOid);
      orderTable.updateColumn(0, dNextOid, oRow);
      orderTable.updateColumn(1, did, oRow);
      orderTable.updateColumn(2, wid, oRow);
      orderTable.updateColumn(3, cid, oRow);
      orderTable.updateColumn(4, generator.getTime(), oRow);
      orderTable.updateColumn(5, 0, oRow);
      orderTable.updateColumn(6, olCnt, oRow);
      orderTable.updateColumn(7, allLocal ? 1 : 0, oRow);
      client.writeAndExecute(TPCCConstants.kOrderTable, orderKey.str(), oRow);

    //   System.out.println("NewOrder " + orderTable.printColumn(oRow));

      // Write order line
      itemId = 0;
      for (Integer iid : iids) {

        olQuantity = olQuantities.get(itemId);
        supplyWid = swids.get(itemId);

        itemKey = new ItemKey(iid);
        results = client.readAndExecute(TPCCConstants.kItemTable, itemKey.str());
        oRow = results.get(0);
        if (oRow.length == 0) {
          // System.out.println("Item not found, rolling back " + iid);
          client.abortTransaction();
          return true;
        }
        price = (Integer) itemTable.getColumn(3, oRow);
        iData = (String) itemTable.getColumn(4, oRow);
        stockKey = new StockKey(supplyWid, iid);
        results = client.readForUpdateAndExecute(TPCCConstants.kStockTable, stockKey.str());
        //   results = client.readAndExecute(TPCCConstants.kStockTable, stockKey.str());
        oRow = results.get(0);
        sQuantity = (Integer) stockTable.getColumn(2, oRow);
        distInfo = (String) stockTable.getColumn(3 + did, oRow);
        sData = (String) stockTable.getColumn(16, oRow);
        sYtd = (Integer) stockTable.getColumn(13, oRow);
        remoteCount = (Integer) stockTable.getColumn(15, oRow);
        sYtd += olQuantity;
        stock = sQuantity - olQuantity;
        if (stock < 0) {
          stock += 91;
        } else {
          stock = sQuantity - olQuantity;
        }
        stockTable.updateColumn(2, stock, oRow);
        stockTable.updateColumn(13, sYtd, oRow);
        stockTable.updateColumn(14, 1, oRow);
        stockTable.updateColumn(15, wid != supplyWid ? remoteCount + 1 : remoteCount, oRow);
        client.updateAndExecute(TPCCConstants.kStockTable, stockKey.str(), oRow);

        orderLineKey = new OrderLineKey(wid, did, dNextOid, itemId);
        oRow = orderLineTable.createNewRow(config.PAD_COLUMNS);
        orderLineTable.updateColumn(0, dNextOid, oRow);
        orderLineTable.updateColumn(1, did, oRow);
        orderLineTable.updateColumn(2, wid, oRow);
        orderLineTable.updateColumn(3, itemId, oRow);
        orderLineTable.updateColumn(4, iid, oRow);
        orderLineTable.updateColumn(5, supplyWid, oRow);
        // ol_deliver_g set to null
        orderLineTable.updateColumn(6, "", oRow);
        orderLineTable.updateColumn(7, olQuantity, oRow);
        amount += (int) ((double) olQuantity * (double) price);
        orderLineTable.updateColumn(8, olQuantity * price, oRow);
        client.writeAndExecute(TPCCConstants.kOrderLineTable, orderLineKey.str(),
            oRow);
        itemId++;
      }
      amount = (int) (amount * (1.0 - (double) cDiscount / 10000.0)
          * (1.0 + (double) dTax / 10000.0 + (double) wTax / 10000.0));

      client.commitTransaction();
    //    System.out.println("[END] New Order ");
      return true;
    } catch (DatabaseAbortException e) {
      // System.out.println("ERROR New Order ");
      return false;
    } catch (Exception e) {
      // TODO(natacha): remove
      System.err.println(e.getMessage());
      System.err.println(e.getStackTrace());
      System.exit(-1);
      try {
        client.abortTransaction();
      } catch (DatabaseAbortException e1) {
        System.exit(-1);
      }
    }
    return false;
  }

}
