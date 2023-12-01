package shield.benchmarks.readwrite;

public class RWConstants {

  /**
  * List of all possible transactions
  */
 public enum Transactions {
        READ_X_WRITE_X,
        READ_Z_WRITE_X
  }

// Names of the various tables and number of columns
  public static String kKeysTable = "keys";
  public static int kKeysCols  = 2;

}
