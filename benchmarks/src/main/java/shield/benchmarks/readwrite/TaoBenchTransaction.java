package shield.benchmarks.readwrite;

import java.util.List;
import java.util.HashSet;
import java.util.Set;
import shield.benchmarks.utils.BenchmarkTransaction;
import shield.benchmarks.utils.Generator;
import shield.benchmarks.ycsb.utils.ZipfianIntGenerator;
import shield.client.DatabaseAbortException;
import shield.client.ClientBase;
import shield.client.schema.Table;

/**
 * a parameterized transaction that represents calculating the total balance for
 * a customer with name N. It returns the sum of savings and checking balances for the specified
 * customer
 */
public class TaoBenchTransaction extends BenchmarkTransaction{
  private final RWExperimentConfiguration config;
  private int[] read_keys = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
  private int[] write_keys = {0};
  private int type;
  private int percent;
  private boolean finalWrite = false;
  private int num_cold_keys = Generator.generateInt(1,10);//5,16);//20,56);
  private int key;
  // private ZipfianIntGenerator keyRecord;

  public TaoBenchTransaction(RWGenerator generator, int key, long txn_id) {
    this.client = generator.getClient();
    this.config = generator.getConfig();

    this.type = Generator.generateInt(0,8);
    this.percent = Generator.generateInt(0,100);
    this.key = key;
    // this.keyRecord = new ZipfianIntGenerator(config.NB_KEYS, config.YCSB_ZIPF);

    if (this.percent < config.TAOBENCH_CONFLICT) {
        this.finalWrite = true;
        this.write_keys[0] = this.type;
    } else {
        this.type = 11;
    }

    if (!finalWrite) {
        // if (Generator.generateInt(0,1) < 1) {
        //     this.num_cold_keys = Generator.generateInt(1,11);
        // }
    }
  }

  @Override
  public boolean tryRun() {
    try {
      List<byte[]> results;

      client.startTransaction();

      byte[] val = Generator.generateBytes(config.VALUE_SIZE);

      int p = Generator.generateInt(0,100);
      if (p > config.TAOBENCH_CONFLICT) {

        int p2 = Generator.generateInt(0,100);
        if (p < config.READ_PERCENT) {
        //   this.type = key;
        //   if (key > 39) {
        //     this.type = 0;
        //   }
        //   if (this.config.SCHEDULE && this.type != 0) {
        //     client.scheduleTransaction(this.type+1); //1);//
        //  }

//          System.out.println("Read");
          int k = Generator.generateInt(500,config.NB_KEYS);
          results = client.readAndExecute("", Integer.toString(k));
          // int len = this.keyRecord.nextValue() + 1;
          // Set<Integer> set = new HashSet<Integer>();
          // for (int i = 0; i < len; i++) {
          //   int key = this.keyRecord.nextValue();
          //   while (set.contains(key)) {
          //     key = this.keyRecord.nextValue();
          //   }
          //   set.add(key);

          // }
        } else {
//          System.out.println("Write");
//          int key = this.keyRecord.nextValue();
          this.type = key + 101;
          if (key > 19) {
            this.type = 0;
          }
          if (this.config.SCHEDULE && this.type != 0) {
            // System.out.println("Schedule: " + (this.type));
            client.scheduleTransaction(this.type); //1);//
          }
          results = client.readForUpdateAndExecute("", Integer.toString(key));

          results = client.writeAndExecute("", Integer.toString(key), val);
        }
      } else {
//        System.out.println("Long");
        this.type = key + 101;
        if (key > 39) {
          this.type = 0;
        }
        if (this.config.SCHEDULE && this.type != 0) {
          // System.out.println("Schedule: " + (this.type));
          client.scheduleTransaction(this.type); //1);//+1+100
        }

        results = client.readAndExecute("", Integer.toString(key));


        Set<Integer> set = new HashSet<Integer>();
        for (int i = 0; i < num_cold_keys; i++) {
          int k = Generator.generateInt(50,config.NB_KEYS);
          while (set.contains(k)) {
              k = Generator.generateInt(50,config.NB_KEYS);
          }
          set.add(k);

          // if (k % 10 < 8) {
              results = client.readAndExecute("", Integer.toString(k));
          // } else {
          //     results = client.writeAndExecute("", Integer.toString(k), val);
          // }
        }


        results = client.writeAndExecute("", Integer.toString(key), val);
      }

      client.commitTransaction();
      return true;

    } catch (DatabaseAbortException e) {
      return false;
    }
  }

//   @Override
//   public boolean tryRun() {
//   try {

// //      System.out.println("ReadX Transaction");

//       List<byte[]> results;

//       client.startTransaction();
//       if (this.config.SCHEDULE && this.type < 10) {
//         client.scheduleTransaction(this.type+1+100); //1);//
//       }

//       byte[] val = Generator.generateBytes(config.VALUE_SIZE);

//       if (finalWrite) {
//           for (int i = 0; i < read_keys.length; i++) {
//               boolean willWrite = false;
//               for (int wk : write_keys) {
//                   if (wk == read_keys[i]) {
//                       willWrite = true;
//                       break;
//                   }
//               }
//               if (willWrite) {
//                   results = client.readForUpdateAndExecute("", Integer.toString(read_keys[i]));
//               }
//           }
//       }


//      for (int i = 0; i < read_keys.length; i++) {
//        boolean willWrite = false;
//        for (int wk : write_keys) {
//            if (wk == read_keys[i]) {
//                willWrite = true;
//                break;
//            }
//        }
//        if (!willWrite) {
//            results = client.readAndExecute("", Integer.toString(read_keys[i]));
//        }
//      }

//       Set<Integer> set = new HashSet<Integer>();
//       for (int i = 0; i < num_cold_keys; i++) {
//         int key = Generator.generateInt(10,config.NB_KEYS);
//         while (set.contains(key)) {
//             key = Generator.generateInt(10,config.NB_KEYS);
//         }
//         set.add(key);

//         if (key % 10 < 8) {
//             results = client.readAndExecute("", Integer.toString(key));
//         } else {
//             results = client.writeAndExecute("", Integer.toString(key), val);
//         }
//       }


//       if (finalWrite) {
//         for (int i = 0; i < write_keys.length; i++) {
//             results = client.writeAndExecute("", Integer.toString(write_keys[i]), val);
//         }
//       }

//       client.commitTransaction();
//       return true;

//     } catch (DatabaseAbortException e) {
//       return false;
    // }
  // }
}
