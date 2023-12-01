package shield.benchmarks.epinions;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import shield.benchmarks.epinions.EpinionsConstants.Transactions;

// import shield.benchmarks.freehealth.FreeHealthExperimentConfiguration;
// import shield.benchmarks.smallbank.AmalgamateTransaction;
// import shield.benchmarks.smallbank.SmallBankConstants;
// import shield.benchmarks.smallbank.SmallBankExperimentConfiguration;
import shield.benchmarks.ycsb.utils.ZipfianIntGenerator;
import shield.benchmarks.utils.BenchmarkTransaction;
import shield.benchmarks.utils.Generator;
import shield.benchmarks.utils.TrxStats;
// import shield.client.RedisPostgresClient;
import shield.client.ClientBase;
import shield.client.schema.ColumnInfo;

import static shield.benchmarks.freehealth.utils.FreeHealthConstants.patientsIdentityByNameTable;

public class EpinionsGenerator {
    private final EpinionsExperimentConfiguration config;

    private final ClientBase client;
    private final HashMap<EpinionsConstants.Transactions, TrxStats> trxStats;
    private final char[] ALPHANUM =  "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz".toCharArray();
    private final int num_users;
    private final int num_items;
    private final int num_reviews;
    private final Random rand = new Random(System.currentTimeMillis());
    private static long txn_id = 0;
    private ZipfianIntGenerator itemRecord;
    private ZipfianIntGenerator userRecord;

    public EpinionsGenerator(ClientBase client, EpinionsExperimentConfiguration config) {
        this.config = config;
        this.client = client;
        this.trxStats = new HashMap<>();
        for (EpinionsConstants.Transactions tType: EpinionsConstants.Transactions.values()) {
            trxStats.put(tType, new TrxStats());
        }
        createEpinionsTables(client);
        this.num_users = config.NUM_USERS;
        this.num_items = config.NUM_ITEMS;
        this.num_reviews = config.NUM_REVEIWS;
        this.itemRecord = new ZipfianIntGenerator(config.NUM_ITEMS, config.ITEM_ZIPF);
        this.userRecord = new ZipfianIntGenerator(config.NUM_USERS, config.USER_ZIPF);
    }

    public ClientBase getClient() {
        return client;
    }

    private void createEpinionsTables(ClientBase client) {

        client.createTable(EpinionsConstants.kUserTable,
                new ColumnInfo("U_U_ID", Integer.class),
                new ColumnInfo("U_NAME", String.class, config.NAME_LENGTH));
        client.createTable(EpinionsConstants.kItemTable,
                new ColumnInfo("I_I_ID", Integer.class),
                new ColumnInfo("I_NAME", String.class, config.TITLE_LENGTH));
        client.createTable(EpinionsConstants.kReviewTable,
                new ColumnInfo("R_A_ID", Integer.class),
                new ColumnInfo("R_U_ID", Integer.class),
                new ColumnInfo("R_I_ID", Integer.class),
                new ColumnInfo("R_RATING", Integer.class),
                new ColumnInfo("R_RANK", Integer.class));
        client.createTable(EpinionsConstants.kReviewByUIDTable, // lookup table for ordering by name
                new ColumnInfo("ID_LIST", String.class, config.UUID_LIST_SIZE));
        client.createTable(EpinionsConstants.kReviewByIIDTable, // lookup table for ordering by name
                new ColumnInfo("ID_LIST", String.class, config.UUID_LIST_SIZE));
        client.createTable(EpinionsConstants.kTrustTable,
                new ColumnInfo("T_SU_ID", Integer.class),
                new ColumnInfo("T_TU_ID", Integer.class),
                new ColumnInfo("T_TRUST", Integer.class),
                new ColumnInfo("T_DATE", Integer.class));

    }

    public void runNextTransaction() {
        int x = Generator.generateInt(0,100);
        int nbAborts;
        long begin = System.currentTimeMillis();
        long end = 0;
        BenchmarkTransaction trx;
        if (x < config.PROB_TRX_REVIEWITEMBYID) {
            trx = GenerateReviewItemByID();
//      System.out.println("[" + Transactions.AMALGAMATE+ "] Begin");
            nbAborts = trx.run();
            end = System.currentTimeMillis();
            trxStats.get(EpinionsConstants.Transactions.REVIEWITEMBYID).addTransaction(nbAborts, end-begin);
//      System.out.println("[" + Transactions.AMALGAMATE + "] End");
        }
        else if (x < config.PROB_TRX_REVIEWITEMBYID + config.PROB_TRX_REVIEWSBYUSER) {
            trx = GenerateReviewsByUser();
//      System.out.println("[" + Transactions.TRANSACT_SAVINGS+ "] Begin");
            nbAborts = trx.run();
            end = System.currentTimeMillis();
//       System.out.println("[" + Transactions.TRANSACT_SAVINGS+ "] End");
            trxStats.get(EpinionsConstants.Transactions.REVIEWSBYUSER).addTransaction(nbAborts, end-begin);
        } else if (x < config.PROB_TRX_REVIEWITEMBYID + config.PROB_TRX_REVIEWSBYUSER +
                config.PROB_TRX_AVGRATINGTRUSTEDUSER) {
            trx = GenerateAverageRatingByTrustedUser();
//      System.out.println("[" + Transactions.SEND_PAYMENT + "] Begin");
            nbAborts = trx.run();
            end = System.currentTimeMillis();
//       System.out.println("[" + Transactions.SEND_PAYMENT+ "] End");
            trxStats.get(EpinionsConstants.Transactions.AVGRATINGTRUSTEDUSER).addTransaction(nbAborts, end-begin);
        } else if (x < config.PROB_TRX_REVIEWITEMBYID + config.PROB_TRX_REVIEWSBYUSER +
                config.PROB_TRX_AVGRATINGTRUSTEDUSER + config.PROB_TRX_AVGRATINGOFITEM){
            trx = GenerateAverageRatingOfItem();
//      System.out.println("[" + Transactions.BALANCE+ "] Begin");
            nbAborts = trx.run();
            end = System.currentTimeMillis();
//       System.out.println("[" + Transactions.BALANCE+ "] End");
            trxStats.get(EpinionsConstants.Transactions.AVGRATINGOFITEM).addTransaction(nbAborts, end-begin);
        } else if (x < config.PROB_TRX_REVIEWITEMBYID + config.PROB_TRX_REVIEWSBYUSER +
                config.PROB_TRX_AVGRATINGTRUSTEDUSER + config.PROB_TRX_AVGRATINGOFITEM +
                config.PROB_TRX_ITEMREVIEWTRUSTEDUSER){
            trx = GenerateItemReviewsByTrustedUser();
//      System.out.println("[" + Transactions.DEPOSIT_CHECKING+ "] Begin");
            nbAborts = trx.run();
            end = System.currentTimeMillis();
//       System.out.println("[" + Transactions.DEPOSIT_CHECKING+ "] End");
            trxStats.get(EpinionsConstants.Transactions.ITEMREVIEWTRUSTEDUSER).addTransaction(nbAborts,end-begin);
        } else if (x < config.PROB_TRX_REVIEWITEMBYID + config.PROB_TRX_REVIEWSBYUSER +
                config.PROB_TRX_AVGRATINGTRUSTEDUSER + config.PROB_TRX_AVGRATINGOFITEM +
                config.PROB_TRX_ITEMREVIEWTRUSTEDUSER + config.PROB_TRX_UPDATEUSERNAME){
            trx = GenerateUpdateUserName();
//      System.out.println("[" + Transactions.DEPOSIT_CHECKING+ "] Begin");
            nbAborts = trx.run();
            end = System.currentTimeMillis();
//       System.out.println("[" + Transactions.DEPOSIT_CHECKING+ "] End");
            trxStats.get(Transactions.UPDATEUSERNAME).addTransaction(nbAborts,end-begin);
        } else if (x < config.PROB_TRX_REVIEWITEMBYID + config.PROB_TRX_REVIEWSBYUSER +
                config.PROB_TRX_AVGRATINGTRUSTEDUSER + config.PROB_TRX_AVGRATINGOFITEM +
                config.PROB_TRX_ITEMREVIEWTRUSTEDUSER + config.PROB_TRX_UPDATEUSERNAME +
                config.PROB_TRX_UPDATEITEMTITLE){
            trx = GenerateUpdateItemTitle();
//      System.out.println("[" + Transactions.DEPOSIT_CHECKING+ "] Begin");
            nbAborts = trx.run();
            end = System.currentTimeMillis();
//       System.out.println("[" + Transactions.DEPOSIT_CHECKING+ "] End");
            trxStats.get(EpinionsConstants.Transactions.UPDATEITEMTITLE).addTransaction(nbAborts,end-begin);
        } else if (x < config.PROB_TRX_REVIEWITEMBYID + config.PROB_TRX_REVIEWSBYUSER +
                config.PROB_TRX_AVGRATINGTRUSTEDUSER + config.PROB_TRX_AVGRATINGOFITEM +
                config.PROB_TRX_ITEMREVIEWTRUSTEDUSER + config.PROB_TRX_UPDATEUSERNAME +
                config.PROB_TRX_UPDATEITEMTITLE + config.PROB_TRX_UPDATEREVIEWRATING){
            trx = GenerateUpdateReviewRating();
//      System.out.println("[" + Transactions.DEPOSIT_CHECKING+ "] Begin");
            nbAborts = trx.run();
            end = System.currentTimeMillis();
//       System.out.println("[" + Transactions.DEPOSIT_CHECKING+ "] End");
            trxStats.get(EpinionsConstants.Transactions.UPDATEREVIEWRATING).addTransaction(nbAborts,end-begin);
        } else if (x < config.PROB_TRX_REVIEWITEMBYID + config.PROB_TRX_REVIEWSBYUSER +
                config.PROB_TRX_AVGRATINGTRUSTEDUSER + config.PROB_TRX_AVGRATINGOFITEM +
                config.PROB_TRX_ITEMREVIEWTRUSTEDUSER + config.PROB_TRX_UPDATEUSERNAME +
                config.PROB_TRX_UPDATEITEMTITLE + config.PROB_TRX_UPDATEREVIEWRATING +
                config.PROB_TRX_UPDATETRUSTRATING) {
            trx = GenerateUpdateTrustRating();
//      System.out.println("[" + Transactions.WRITE_CHECK+ "] Begin");
            nbAborts = trx.run();
            end = System.currentTimeMillis();
//       System.out.println("[" + Transactions.WRITE_CHECK+ "] End");
            trxStats.get(EpinionsConstants.Transactions.UPDATETRUSTRATING).addTransaction(nbAborts,end-begin);
        } else {
            trx = GenerateUpdateItemAvg();
            nbAborts = trx.run();
            end = System.currentTimeMillis();
            trxStats.get(EpinionsConstants.Transactions.UPDATEITEMAVERAGE).addTransaction(nbAborts,end-begin);
        }
    }

    public ReviewItemByIDTransaction GenerateReviewItemByID() {
        // int iid = rand.nextInt(num_items);
        int iid = this.itemRecord.nextValue();
        return new ReviewItemByIDTransaction(this,iid,txn_id++);
    }

    public ReviewsByUserTransaction GenerateReviewsByUser() {
        // int uid = rand.nextInt(num_users);
        int uid = this.userRecord.nextValue();
        return new ReviewsByUserTransaction(this,uid,txn_id++);
    }

    public AverageRatingByTrustedUserTransaction GenerateAverageRatingByTrustedUser() {
        // int iid = rand.nextInt(num_items);
        int iid = this.itemRecord.nextValue();
        // int uid = rand.nextInt(num_users);
        int uid = this.userRecord.nextValue();
        return new AverageRatingByTrustedUserTransaction(this,iid,uid,txn_id++);
    }

    public AverageRatingOfItemTransaction GenerateAverageRatingOfItem() {
        // int iid = rand.nextInt(num_items);
        int iid = this.itemRecord.nextValue();
        return new AverageRatingOfItemTransaction(this,iid,txn_id++);
    }

    public ItemReviewsByTrustedUserTransaction GenerateItemReviewsByTrustedUser() {
        // int iid = rand.nextInt(num_items);
        int iid = this.itemRecord.nextValue();
        // int uid = rand.nextInt(num_users);
        int uid = this.userRecord.nextValue();
        return new ItemReviewsByTrustedUserTransaction(this,iid,uid,txn_id++);
    }

    public UpdateUserNameTransaction GenerateUpdateUserName() {
        // int uid = rand.nextInt(num_users);
        int uid = this.userRecord.nextValue();
        String title = RandString(config.NAME_LENGTH, config.NAME_LENGTH, false);
        return new UpdateUserNameTransaction(this,uid,title,txn_id++);
    }

    public UpdateItemTitleTransaction GenerateUpdateItemTitle() {
        // int iid = rand.nextInt(num_items);
        int iid = this.itemRecord.nextValue();
        String title = RandString(config.TITLE_LENGTH, config.TITLE_LENGTH, false);
        return new UpdateItemTitleTransaction(this,iid,title,txn_id++);
    }

    public UpdateReviewRatingTransaction GenerateUpdateReviewRating() {
        // int iid = rand.nextInt(num_items);
        int iid = this.itemRecord.nextValue();
        // int uid = rand.nextInt(num_users);
        int uid = this.userRecord.nextValue();
        int rating = rand.nextInt(1000);
        return new UpdateReviewRatingTransaction(this,iid,uid,rating,txn_id++);
    }

    public UpdateTrustRatingTransaction GenerateUpdateTrustRating() {
        // int uid = rand.nextInt(num_users);
        int uid = this.userRecord.nextValue();
        int uid2 = rand.nextInt(num_users);
        int trust = rand.nextInt(2);
        return new UpdateTrustRatingTransaction(this,uid,uid2,trust,txn_id++);
    }

    public UpdateItemAvgTransaction GenerateUpdateItemAvg() {
        // int iid = rand.nextInt(num_items);
        int iid = this.itemRecord.nextValue();
        String title = RandString(config.TITLE_LENGTH, config.TITLE_LENGTH, false);
        return new UpdateItemAvgTransaction(this,iid,title,txn_id++);
    }

    public EpinionsExperimentConfiguration getConfig() {
        return config;
    }

    public HashMap<EpinionsConstants.Transactions, TrxStats> getTrxStats() {
        return trxStats;
    }

    public char RandCharNum(boolean numOnly) {
        int x = Generator.generateInt(0,numOnly?10:26);
        return ALPHANUM[x];
    }

    /**
     * Generates a random string of size between min and max, and optionally consisting
     * of numbers only
     *
     * @param num_only
     * @return
     */
    public String RandString(int min, int max, boolean num_only) {
        StringBuffer bf = new StringBuffer();
        int len = Generator.generateInt(min, max);
        for (int i = 0; i < len; ++i) {
            bf.append(RandCharNum(num_only));
        }
        return bf.toString();
    }

    public void printStats() {
        trxStats.forEach((tType,stat) -> System.out.println("[STAT] " + tType + " " +  stat.getStats()));
    }
}
