package shield.benchmarks.epinions;

import java.io.FileReader;
import java.io.IOException;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import shield.benchmarks.utils.ClientUtils;
import shield.benchmarks.utils.ClientUtils.ClientType;
import shield.config.Configuration;

/**
 * All configuration variables necessary to setup an experiment should be placed here and loadable
 * from JSON The loadProperty() function is called once at the initialization of each block
 *
 * @author ncrooks
 */

public class EpinionsExperimentConfiguration extends Configuration {

    public int ID_SIZE=8; // max number of digits in an ID (determines padding for index serialization)
    public int UUID_LIST_SIZE=200; // size of ID Lists @accheng: does this need to be bigger?

    public int THREADS = 1;

    public int REQ_THREADS_PER_BM_THREAD = 3;

    /**
     * Warm-up period before which results start being collected
     */
    public int RAMP_UP = 60;

    /**
     * Ramp-down period during which results are no longer collected
     */
    public int RAMP_DOWN = 15;

    /**
     * Total experiment duration (including ramp up, ramp down)
     */
    public int EXP_LENGTH = 90;

    /**
     * Name of this run (used to determine where collected data will be outputted)
     */
    public String RUN_NAME = "";

    /**
     * Experiment dir
     */
    public String EXP_DIR = "";

    /**
     * Name of the file in which the keys are stored. If the file name remains "" after load,
     */
    public String KEY_FILE_NAME = "";

    /**
     * Number of loader threads
     */
    public int NB_LOADER_THREADS = 16;

    /**
     * True if must pad columns
     */

    public boolean PAD_COLUMNS = true;
    /**
     * True if must load keys
     */
    public boolean MUST_LOAD_KEYS = true;

    public boolean SCHEDULE = false;

    public double ITEM_ZIPF = 1.95;

    public double USER_ZIPF = 1.95;

    public int NB_CLIENT_THREADS = 16;

    // Constants
    public int NUM_USERS = 2000; // Number of baseline Users
    public int NUM_ITEMS = 1000; // Number of baseline pages
    public int NUM_REVEIWS = 2000;

    public int NAME_LENGTH = 5; // Length of user's name
    public int TITLE_LENGTH = 20;

    public int REVIEW = 500; // this is the average .. expand to max
    public int TRUST = 200; // this is the average .. expand to max
    public int REVIEW_INCR = 1;

    public final static int BATCH_SIZE = 1000;

    public double PROB_TRX_REVIEWITEMBYID = 10;

    public double PROB_TRX_REVIEWSBYUSER = 10;

    public double PROB_TRX_AVGRATINGTRUSTEDUSER  = 10;

    public double PROB_TRX_AVGRATINGOFITEM = 10;

    public double PROB_TRX_ITEMREVIEWTRUSTEDUSER = 10;

    public double PROB_TRX_UPDATEUSERNAME = 10;

    public double PROB_TRX_UPDATEITEMTITLE = 10;

    public double PROB_TRX_UPDATEREVIEWRATING = 10;

    public double PROB_TRX_UPDATETRUSTRATING = 20;

    /**
     * Probability to execute amalgamate trx
     */
    public double PROB_TRX_AMALGAMATE= 0;//15.0;
    /**
     * Probability to execute new order transaction
     */
    public double PROB_TRX_BALANCE = 100;//15.0;
    /**
     * Probability to execute order status transaction
     */
    public double PROB_TRX_DEPOSIT_CHECKING = 0;//15.0;
    /**
     * Probability to execute stock level transaction
     */
    public double PROB_TRX_SEND_PAYMENT= 0;//25.0;
    /**
     * Probability to execute payment transaction
     */
    public double PROB_TRX_TRANSACT_SAVINGS= 0;//15.0;

    /**
     *Probability that access a hotspot account
     */
    public int PROB_ACCOUNT_HOTSPOT  = 0;//90;

    /**
     * Number of accounts
     */
    public int NB_ACCOUNTS = 1000000;

    public boolean HOTSPOT_USE_FIXED_SIZE  = false;
    public double HOTSPOT_PERCENTAGE = 0;//10; // [0% - 100%]
    public int HOTSPOT_FIXED_SIZE  = 100; // fixed number of tuples

    // Initial balance amount
    // We'll just make it really big so that they never run out of money
    public int MIN_BALANCE             = 10000;
    public int MAX_BALANCE             = 50000;
    public int PARAM_SEND_PAYMENT_AMOUNT = 5;
    public int PARAM_DEPOSIT_CHECKING_AMOUNT = 1;
    public int PARAM_TRANSACT_SAVINGS_AMOUNT = 20;
    public int PARAM_WRITE_CHECK_AMOUNT = 5;

    public boolean USE_THINK_TIME = true;
    public int THINK_TIME = 10;

    /**
     * Max size of the variable metadata column
     */
    public int VAR_DATA_SIZE = 93;
    public int NAME_SIZE=64;

    public ClientUtils.ClientType CLIENT_TYPE = ClientUtils.ClientType.SHIELD;

    public EpinionsExperimentConfiguration(String configFileName)
            throws IOException, ParseException {
        loadProperties(configFileName);
    }

    public EpinionsExperimentConfiguration() {
        loadProperties();
    }


    /**
     * Loads the constant values from JSON file
     */
    public void loadProperties(String fileName)
            throws IOException, ParseException {

        isInitialised = true;

        FileReader reader = new FileReader(fileName);
        if (fileName == "") {
            System.err.println("Empty Property File, Intentional?");
        }
        JSONParser jsonParser = new JSONParser();
        JSONObject prop = (JSONObject) jsonParser.parse(reader);

        // THREADS = getPropInt(prop, "threads", THREADS);
        REQ_THREADS_PER_BM_THREAD = getPropInt(prop, "req_threads_per_bm_thread", REQ_THREADS_PER_BM_THREAD);
        RAMP_UP = getPropInt(prop, "ramp_up", RAMP_UP);
        RAMP_DOWN = getPropInt(prop, "ramp_down", RAMP_DOWN);
        EXP_LENGTH = getPropInt(prop, "exp_length", EXP_LENGTH);
        RUN_NAME = getPropString(prop, "run_name", RUN_NAME);
        EXP_DIR = getPropString(prop, "exp_dir", EXP_DIR);
        KEY_FILE_NAME = getPropString(prop, "key_file_name", KEY_FILE_NAME);
        NB_LOADER_THREADS = getPropInt(prop, "nb_loader_threads", NB_LOADER_THREADS);
        PAD_COLUMNS = getPropBool(prop, "pad_columns", PAD_COLUMNS);
        CLIENT_TYPE = ClientUtils.fromStringToClientType(getPropString(prop, "client_type", ""));
        PROB_TRX_AMALGAMATE= getPropDouble(prop, "prob_trx_amalgamate", PROB_TRX_AMALGAMATE);
        PROB_TRX_BALANCE=getPropDouble(prop, "prob_trx_balance", PROB_TRX_BALANCE);
        PROB_TRX_DEPOSIT_CHECKING= getPropDouble(prop, "prob_trx_deposit_checking", PROB_TRX_DEPOSIT_CHECKING);
        PROB_TRX_SEND_PAYMENT=getPropDouble(prop, "prob_trx_send_payment", PROB_TRX_SEND_PAYMENT);
        PROB_TRX_TRANSACT_SAVINGS= getPropDouble(prop, "prob_trx_transact_savings", PROB_TRX_TRANSACT_SAVINGS);
        NB_ACCOUNTS = getPropInt(prop, "nb_accounts", NB_ACCOUNTS);
        HOTSPOT_PERCENTAGE = getPropDouble(prop, "hotspot_percentage", HOTSPOT_PERCENTAGE);
        PROB_ACCOUNT_HOTSPOT = getPropInt(prop, "prob_account_hotspot", PROB_ACCOUNT_HOTSPOT);
        USE_THINK_TIME = getPropBool(prop, "use_think_time", USE_THINK_TIME);
        THINK_TIME = getPropInt(prop, "think_time", THINK_TIME);
        NUM_USERS = getPropInt(prop, "num_users", NUM_USERS);
        NUM_ITEMS = getPropInt(prop, "num_items", NUM_ITEMS);
        NUM_REVEIWS = getPropInt(prop, "num_reviews", NUM_REVEIWS);
        REVIEW_INCR = getPropInt(prop, "review_inr", REVIEW_INCR);
        PROB_TRX_REVIEWITEMBYID = getPropDouble(prop, "prob_trx_reviewitemid", PROB_TRX_REVIEWITEMBYID);
        PROB_TRX_REVIEWSBYUSER = getPropDouble(prop, "prob_trx_reviewuser", PROB_TRX_REVIEWSBYUSER);
        PROB_TRX_AVGRATINGTRUSTEDUSER = getPropDouble(prop, "prob_trx_avgusertrust", PROB_TRX_AVGRATINGTRUSTEDUSER);
        PROB_TRX_AVGRATINGOFITEM = getPropDouble(prop, "prob_trx_avgratingitem", PROB_TRX_AVGRATINGOFITEM);
        PROB_TRX_ITEMREVIEWTRUSTEDUSER = getPropDouble(prop, "prob_trx_itemreview", PROB_TRX_ITEMREVIEWTRUSTEDUSER);
        PROB_TRX_UPDATEUSERNAME = getPropDouble(prop, "prob_trx_updateuser", PROB_TRX_UPDATEUSERNAME);
        PROB_TRX_UPDATEITEMTITLE = getPropDouble(prop, "prob_trx_updateitem", PROB_TRX_UPDATEITEMTITLE);
        PROB_TRX_UPDATEREVIEWRATING = getPropDouble(prop, "prob_trx_updatereview", PROB_TRX_UPDATEREVIEWRATING);
        PROB_TRX_UPDATETRUSTRATING = getPropDouble(prop, "prob_trx_updatetrust", PROB_TRX_UPDATETRUSTRATING);
        MUST_LOAD_KEYS = getPropBool(prop, "must_load_keys", MUST_LOAD_KEYS);
        SCHEDULE = getPropBool(prop, "must_schedule", SCHEDULE);
        ITEM_ZIPF = getPropDouble(prop, "item_zipf", ITEM_ZIPF);
        USER_ZIPF = getPropDouble(prop, "user_zipf", USER_ZIPF);
        NB_CLIENT_THREADS = getPropInt(prop, "nb_client_threads", NB_CLIENT_THREADS);
    }

    /**
     * This is a test method which initializes constants to default values without the need to pass in
     * a configuration file
     *
     * @return true if initialization successful
     */
    public void loadProperties() {

        isInitialised = true;

    }

}
