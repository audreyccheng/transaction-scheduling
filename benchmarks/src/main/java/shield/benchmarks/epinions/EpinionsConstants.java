package shield.benchmarks.epinions;

public class EpinionsConstants {
    /**
     * List of all possible transactions
     */
    public enum Transactions {
        REVIEWITEMBYID,
        REVIEWSBYUSER,
        AVGRATINGTRUSTEDUSER,
        AVGRATINGOFITEM,
        ITEMREVIEWTRUSTEDUSER,
        UPDATEUSERNAME,
        UPDATEITEMTITLE,
        UPDATEREVIEWRATING,
        UPDATETRUSTRATING,
        UPDATEITEMAVERAGE,
    }

    // Names of the various tables and number of columns
    public static String kUserTable = "useracct";
    public static int kUserCols  = 2;
    public static String kItemTable = "item";
    public static int kItemCols = 2;
    public static String kReviewTable = "review";
    public static int kReviewCols = 5;
    public static String kReviewByUIDTable = "reviewuid";
    public static int kReviewbyUIDCols = 2;
    public static String kReviewByIIDTable = "reviewiid";
    public static int kReviewbyIIDCols = 2;
    public static String kTrustTable = "trust";
    public static int kTrustCols = 4;
}
