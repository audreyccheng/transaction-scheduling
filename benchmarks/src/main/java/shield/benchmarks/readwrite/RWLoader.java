package shield.benchmarks.readwrite;

import static shield.benchmarks.utils.Generator.generatePortNumber;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.json.simple.parser.ParseException;
import shield.benchmarks.utils.ClientUtils;
import shield.client.DatabaseAbortException;
import shield.client.ClientBase;
import shield.config.NodeConfiguration;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import shield.util.Utility;

/**
 * Generates, based on the config parameters the appropriate set of keys
 *
 * @author ncrooks
 */
public class RWLoader {

  public static void main(String[] args) throws IOException, ParseException,
      DatabaseAbortException, InterruptedException {
    String expConfigFile;
    RWExperimentConfiguration rwConfig;

    if (args.length != 1) {
      System.out.println(args.length);
      System.out.println(args[0]);
      System.out.println(args[1]);
      System.err.println(
          "Incorrect number of arguments: expected <clientConfigFile.json expConfigFile.json>");
    }
    // Contains the experiment parameters
    expConfigFile = args[0];
    System.err.println(expConfigFile);
    rwConfig = new RWExperimentConfiguration(expConfigFile);

    System.out.println("Begin loading data");
    loadData(rwConfig, expConfigFile);

    System.out.println("Data loaded");
    System.exit(0);

  }

  private static void loadData(
      RWExperimentConfiguration rwConfig, String expConfigFile)
      throws InterruptedException, IOException, ParseException {
    List<String> keys = new LinkedList<String>();
    for (int i = 0; i < rwConfig.NB_KEYS; i++) {
        keys.add(Integer.toString(i));
    }
    NodeConfiguration nodeConfig = new NodeConfiguration(expConfigFile);
    final int ranges = keys.size() / rwConfig.NB_LOADER_THREADS;
    List<Thread> threads = new LinkedList<Thread>();
    byte[] value = new byte[rwConfig.VALUE_SIZE];
    new Random().nextBytes(value);

    // Pre initialise set of ports to avoid risk of duplicates
    Set<Integer> ports = new HashSet<>();
    while (ports.size() < rwConfig.NB_LOADER_THREADS) {
      ports.add(generatePortNumber());
    }

    Iterator<Integer> it = ports.iterator();
    for (int i = 0; i < keys.size(); i += ranges) {
      System.out.println("Begin Loading" + i);
      final int j = i;
      int id = it.next();
      Thread t = new Thread() {
        public void run() {
          try {
            int maxData = j + ranges > keys.size() ? keys.size() : j + ranges;
            loadSubData(j, maxData, keys, value, id,expConfigFile);
          } catch (Exception e) {
            e.printStackTrace();
            System.err.println(e);
            System.err.println("Loading failed");
            System.exit(-1);
          }
        }
      };
      threads.add(t);
      t.start();
    }

    for (Thread t : threads) {
      t.join();
    }
  }


  private static void loadSubData(int start, int end, List<String> keys,
      byte[] value, int port, String expConfigFile) throws InterruptedException,
      IOException, ParseException, DatabaseAbortException, SQLException {

    System.out.println("Loading Sub data " + start + " " + end);
    int trxSize = 5;

    RWExperimentConfiguration rwConfig = new RWExperimentConfiguration(expConfigFile);
    ClientBase client = ClientUtils.createClient(rwConfig.CLIENT_TYPE, expConfigFile, port, port);
    client.registerClient();

    System.out.println("Client registered " + client.getConfig().NODE_LISTENING_PORT);

    int nbTrxs = 0;

    int i = start;
    while (i < end) {
      System.out.println("I " + i + " " + end);
      boolean success = false;
      System.out.println("Start Transaction " + client.getBlockId() +  " " + nbTrxs);
      while (!success) {
        int ii = 0;
        try {
          client.startTransaction();
          while ((ii < trxSize) && ((i + ii) < end)) {
            String key = keys.get(i+ii);
            client.writeAndExecute("", key, value);
            ii++;
          }
          client.commitTransaction();
          success = true;
        } catch (DatabaseAbortException e) {
          System.out.println("Transaction aborted. Retrying " + client.getBlockId());
          System.err.println("Transaction aborted. Retrying " + client.getBlockId());
          success = false;
        }
      }
      nbTrxs++;
      i += trxSize;
    }

    System.out.println("Finished Loading Sub data " + client.getBlockId() + " " + start + " " + end);
  }
}
