package shield.benchmarks.epinions;

import static shield.benchmarks.utils.Generator.generatePortNumber;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.json.simple.parser.ParseException;
import org.mapdb.Atomic;
import shield.benchmarks.epinions.SerializableIDSet;
import shield.benchmarks.utils.ClientUtils;
import shield.benchmarks.utils.Generator;
import shield.client.ClientBase;
import shield.client.DatabaseAbortException;
// import shield.client.RedisPostgresClient;
import shield.client.ClientBase;
import shield.client.schema.Table;
import shield.benchmarks.ycsb.utils.ZipfianIntGenerator;
import shield.benchmarks.ycsb.utils.ScrambledZipfianGenerator;

public class EpinionsLoader {
    public static void main(String[] args) throws IOException, ParseException,
            DatabaseAbortException, InterruptedException {
        String expConfigFile;
        EpinionsExperimentConfiguration sbConfig;

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
        sbConfig = new EpinionsExperimentConfiguration(expConfigFile);

        if (sbConfig.MUST_LOAD_KEYS) {
            System.out.println("Begin loading data");
            loadData(sbConfig, expConfigFile);
        }

        System.out.println("Data loaded");
        System.exit(0);
    }

    public static void loadData(EpinionsExperimentConfiguration smallBankConfig, String expConfigFile)
            throws InterruptedException, IOException, ParseException {


        // First load data
        int ranges  = (smallBankConfig.NUM_USERS + smallBankConfig.NUM_ITEMS) / smallBankConfig.NB_LOADER_THREADS;
        int accountsToLoad =  ranges > 0? ranges: 1;
        List<Thread> threads = new LinkedList<Thread>();

        // Pre initialise set of ports to avoid risk of duplicates
        Set<Integer> ports = new HashSet<>();
        while (ports.size() < smallBankConfig.NB_LOADER_THREADS) {
            ports.add(generatePortNumber());
        }

        Iterator<Integer> it = ports.iterator();

        AtomicLong progressCount = new AtomicLong(0);
        long begin = System.nanoTime();
//        for (int i = 0 ; i < smallBankConfig.NUM_USERS; i +=  accountsToLoad) {
//            final int j = i;
//            int port = it.next();
//            Thread t = new Thread() {
//                public void run() {
//                    try {
//                        int endAccount=
//                                (j + accountsToLoad > smallBankConfig.NUM_USERS) ? smallBankConfig.NUM_USERS:
//                                        (j + accountsToLoad);
//                        loadUsers(j, endAccount,port,expConfigFile, progressCount);
//                    } catch (Exception e) {
//                        e.printStackTrace();
//                        System.err.println(e);
//                        System.err.println("Loading failed");
//                        System.exit(-1);
//                    }
//                }
//            };
//            threads.add(t);
//            t.start();
//        }
//
//        for (int i = 0 ; i < smallBankConfig.NUM_ITEMS; i +=  accountsToLoad) {
//            final int j = i;
//            int port = it.next();
//            Thread t = new Thread() {
//                public void run() {
//                    try {
//                        int endAccount=
//                                (j + accountsToLoad > smallBankConfig.NUM_ITEMS) ? smallBankConfig.NUM_ITEMS:
//                                        (j + accountsToLoad);
//                        loadItems(j, endAccount,port,expConfigFile, progressCount);
//                    } catch (Exception e) {
//                        e.printStackTrace();
//                        System.err.println(e);
//                        System.err.println("Loading failed");
//                        System.exit(-1);
//                    }
//                }
//            };
//            threads.add(t);
//            t.start();
//        }

        int port1 = it.next();
        Thread t1 = new Thread() {
            public void run() {
                try {
                    loadTrustSeq(port1,expConfigFile, progressCount);
                } catch (Exception e) {
                    e.printStackTrace();
                    System.err.println(e);
                    System.err.println("Loading failed");
                    System.exit(-1);
                }
            }
        };
        threads.add(t1);
        t1.start();

//        for (int i = 0 ; i < smallBankConfig.NUM_USERS; i +=  accountsToLoad) {
//            final int j = i;
//            int port = it.next();
//            Thread t = new Thread() {
//                public void run() {
//                    try {
//                        int endAccount=
//                                (j + accountsToLoad > smallBankConfig.NUM_USERS) ? smallBankConfig.NUM_USERS:
//                                        (j + accountsToLoad);
//                        loadTrust(j, endAccount,port,expConfigFile, progressCount);
//                    } catch (Exception e) {
//                        e.printStackTrace();
//                        System.err.println(e);
//                        System.err.println("Loading failed");
//                        System.exit(-1);
//                    }
//                }
//            };
//            threads.add(t);
//            t.start();
//        }
//
//        for (int i = 0 ; i < smallBankConfig.NUM_ITEMS; i +=  accountsToLoad) {
//            final int j = i;
//            int port = it.next();
//            Thread t = new Thread() {
//                public void run() {
//                    try {
//                        int endAccount=
//                                (j + accountsToLoad > smallBankConfig.NUM_USERS) ? smallBankConfig.NUM_USERS:
//                                        (j + accountsToLoad);
//                        loadReviews(j, endAccount,port,expConfigFile, progressCount);
//                    } catch (Exception e) {
//                        e.printStackTrace();
//                        System.err.println(e);
//                        System.err.println("Loading failed");
//                        System.exit(-1);
//                    }
//                }
//            };
//            threads.add(t);
//            t.start();
//        }

//        int port2 = it.next();
//        Thread t2 = new Thread() {
//            public void run() {
//                try {
//                    loadReviewsSeq(port2,expConfigFile, progressCount);
//                } catch (Exception e) {
//                    e.printStackTrace();
//                    System.err.println(e);
//                    System.err.println("Loading failed");
//                    System.exit(-1);
//                }
//            }
//        };
//        threads.add(t2);
//        t2.start();


        for (Thread t : threads) {
            t.join();
        }

        long end = System.nanoTime();
        System.out.println("Took " + ((double)end-begin)/1000000000 + " seconds to load data");

    }


    private static void loadUsers(int start, int end, int port, String expConfigFile, AtomicLong count)
            throws InterruptedException, ParseException, IOException, DatabaseAbortException, SQLException {

        EpinionsExperimentConfiguration config = new EpinionsExperimentConfiguration(expConfigFile);
        ClientBase client = ClientUtils.createClient(config.CLIENT_TYPE,expConfigFile, port, port);

        assert (end <= config.NUM_USERS);

        System.out.println("Loading Users: " + start + " to " + end);

        client.registerClient();
        EpinionsGenerator generator = new EpinionsGenerator(client, config);


        Table userTable = client.getTable(EpinionsConstants.kUserTable);
        Integer custId;

        boolean success;
        byte[] row;

        int total = 0;
        int batch = 0;
        while (start < end) {
            success = false;
            int oldStart = start;
            while (!success) {
                start = oldStart;
                try {
                    client.startTransaction();
                    for (int ij = 0; ij < 10; ij++) {
                        custId = start + ij;
                        if (custId>= end)
                            break;
                        // Update account table
                        row = userTable.createNewRow(config.PAD_COLUMNS);
                        userTable.updateColumn("U_U_ID", custId, row);
                        userTable.updateColumn("U_NAME",
                                generator.RandString(config.NAME_LENGTH, config.NAME_LENGTH, false), row);
                        client.write(EpinionsConstants.kUserTable, custId.toString(), row);
                    }
                    System.out.println(count.incrementAndGet() * 10 + "/" + config.NUM_USERS + " User Start: " + start);

                    start += 10;
                    client.commitTransaction();
                    success = true;
                } catch (DatabaseAbortException e) {
                    System.err.println("Trx Aborted ");
                    success = false;
                } catch (Exception e) {
                    e.printStackTrace();
                    System.err.println(e.getMessage());
                    success = false;
                    System.err.println("Retrying");
                }
            }
        }

//        client.requestExecutor.shutdown();
//        while (!client.requestExecutor.isTerminated()) {}
    }

    private static void loadItems(int start, int end, int port, String expConfigFile, AtomicLong count)
            throws InterruptedException, ParseException, IOException, DatabaseAbortException, SQLException {

        EpinionsExperimentConfiguration config = new EpinionsExperimentConfiguration(expConfigFile);
        ClientBase client = ClientUtils.createClient(config.CLIENT_TYPE,expConfigFile, port, port);

        assert (end <= config.NUM_ITEMS);

        System.out.println("Loading Items: " + start + " to " + end);

        client.registerClient();
        EpinionsGenerator generator = new EpinionsGenerator(client, config);

        Table itemTable = client.getTable(EpinionsConstants.kItemTable);
        Integer custId;

        boolean success;
        byte[] row;

        int total = 0;
        int batch = 0;
        while (start < end) {
            success = false;
            int oldStart = start;
            while (!success) {
                start = oldStart;
                try {
                    client.startTransaction();
                    for (int ij = 0; ij < 10; ij++) {
                        custId = start + ij;
                        if (custId >= end)
                            break;
                        // Update account table
                        row = itemTable.createNewRow(config.PAD_COLUMNS);
                        itemTable.updateColumn("I_I_ID", custId, row);
                        itemTable.updateColumn("I_NAME",
                                generator.RandString(config.TITLE_LENGTH, config.TITLE_LENGTH, false), row);
                        client.write(EpinionsConstants.kItemTable, custId.toString(), row);
                    }
                    System.out.println(count.incrementAndGet() * 10 + "/" + config.NUM_ITEMS + " Item Start: " + start);

                    start += 10;
                    client.commitTransaction();
                    success = true;
                } catch (DatabaseAbortException e) {
                    System.err.println("Trx Aborted ");
                    success = false;
                } catch (Exception e) {
                    e.printStackTrace();
                    System.err.println(e.getMessage());
                    success = false;
                    System.err.println("Retrying");
                }
            }
        }

//        client.requestExecutor.shutdown();
//        while (!client.requestExecutor.isTerminated()) {}
    }

    private static void loadReviews(int start, int end, int port, String expConfigFile, AtomicLong count)
            throws InterruptedException, ParseException, IOException, DatabaseAbortException, SQLException {

        EpinionsExperimentConfiguration config = new EpinionsExperimentConfiguration(expConfigFile);
        ClientBase client = ClientUtils.createClient(config.CLIENT_TYPE,expConfigFile, port, port);

        System.out.println("Loading Reviews");

        client.registerClient();
        EpinionsGenerator generator = new EpinionsGenerator(client, config);

        Table reviewTable = client.getTable(EpinionsConstants.kReviewTable);
        Table reviewByUIDTable = client.getTable(EpinionsConstants.kReviewByUIDTable);
        Table reviewByIIDTable = client.getTable(EpinionsConstants.kReviewByIIDTable);
        Integer custId;

//        byte[] valueRow = reviewByIIDTable.createNewRow(config.PAD_COLUMNS);
//        client.write(reviewByIIDTable.getTableName(), "1", valueRow);
//        System.out.println("test");

        ZipfianIntGenerator numReviews = new ZipfianIntGenerator(config.REVIEW, 1.8);
        ZipfianIntGenerator reviewer = new ZipfianIntGenerator(config.NUM_USERS);

        boolean success;
        byte[] row;

//        while (start < end) {
//            success = false;
//            int oldStart = start;
//            while (!success) {
//                start = oldStart;
//                try {
//                    client.startTransaction();
//                    for (int ij = 0; ij < 10; ij++) {
//                        custId = start + ij;
//                        if (custId>= end)
//                            break;

        Integer total = 0;
//        for (int i = 0; i < config.NUM_ITEMS; i++) {
        Integer i = start;
        while (start < end) {
            List<Integer> reviewers = new ArrayList<Integer>();
            int review_count = numReviews.nextValue();
            if (review_count == 0)
                review_count = 1;
            success = false;
            int oldTotal = total;
            int oldI = i;
            int oldStart = start;
            while (!success) {
                start = oldStart;
                total = oldTotal;
                i = oldI;
                reviewers = new ArrayList<Integer>();
                try {
                    client.startTransaction();
                    for (int rc = 0; rc < review_count; ) {
                        if (i >= end)
                            break;
                        int u_id = reviewer.nextValue();
                        if (!reviewers.contains(u_id)) {
                            rc++;
                            row = reviewTable.createNewRow(config.PAD_COLUMNS);
                            reviewTable.updateColumn("R_A_ID", total, row);
                            reviewTable.updateColumn("R_U_ID", u_id, row);
                            reviewTable.updateColumn("R_I_ID", i, row);
                            reviewTable.updateColumn("R_RATING", new Random().nextInt(5), row);
                            reviewTable.updateColumn("R_RANK", 0, row);
                            client.write(EpinionsConstants.kReviewTable, total.toString(), row);
                            reviewers.add(u_id);
                            total++;


//                            if (start == 0 && rc == 0) {
//                                row = reviewByIIDTable.createNewRow(config.PAD_COLUMNS);
//                                reviewByIIDTable.updateColumn("ID_LIST",
//                                        new shield.benchmarks.epinions.SerializableIDSet(config).serialize(), row);
//                                client.write(EpinionsConstants.kReviewByIIDTable, Integer.toString(u_id), row);
//                                row = reviewByUIDTable.createNewRow(config.PAD_COLUMNS);
//                                reviewByUIDTable.updateColumn("ID_LIST",
//                                        new shield.benchmarks.epinions.SerializableIDSet(config).serialize(), row);
//                                client.write(EpinionsConstants.kReviewByUIDTable, Integer.toString(i), row);
//                            }

                            addToIndexList(client, config, reviewByUIDTable, Integer.toString(u_id), total);
                            addToIndexList(client, config, reviewByIIDTable, Integer.toString(i), total);
                        }
//                        System.out.println("user " + i + " rc " + rc);
                        System.out.println(count.incrementAndGet() * rc + "/" + config.NUM_ITEMS + " Review Start: " + start);
                    }
                    i++;
                    start++;
                    client.commitTransaction();
                    success = true;
                } catch (DatabaseAbortException e) {
                    System.err.println("Trx Aborted ");
                    success = false;
                } catch (Exception e) {
                    e.printStackTrace();
                    System.err.println(e.getMessage());
                    success = false;
                    System.err.println("Retrying");
                }
            }
            System.out.print("Loaded " + (total - oldTotal) + " rows for users " + i + "\n");
        }
        System.out.println("Loaded " + total + " total");

//        client.requestExecutor.shutdown();
//        while (!client.requestExecutor.isTerminated()) {}
    }

    private static void loadReviewsSeq(int port, String expConfigFile, AtomicLong count)
            throws InterruptedException, ParseException, IOException, DatabaseAbortException, SQLException {

        EpinionsExperimentConfiguration config = new EpinionsExperimentConfiguration(expConfigFile);
        ClientBase client = ClientUtils.createClient(config.CLIENT_TYPE,expConfigFile, port, port);

        System.out.println("Loading Reviews");

        client.registerClient();
        EpinionsGenerator generator = new EpinionsGenerator(client, config);

        Table reviewTable = client.getTable(EpinionsConstants.kReviewTable);
        Table reviewByUIDTable = client.getTable(EpinionsConstants.kReviewByUIDTable);
        Table reviewByIIDTable = client.getTable(EpinionsConstants.kReviewByIIDTable);
        Integer custId;

//        byte[] valueRow = reviewByIIDTable.createNewRow(config.PAD_COLUMNS);
//        client.write(reviewByIIDTable.getTableName(), "1", valueRow);
//        System.out.println("test");

        ZipfianIntGenerator numReviews = new ZipfianIntGenerator(config.REVIEW, 1.8);
        ZipfianIntGenerator reviewer = new ZipfianIntGenerator(config.NUM_USERS);

        boolean success;
        byte[] row;

//        while (start < end) {
//            success = false;
//            int oldStart = start;
//            while (!success) {
//                start = oldStart;
//                try {
//                    client.startTransaction();
//                    for (int ij = 0; ij < 10; ij++) {
//                        custId = start + ij;
//                        if (custId>= end)
//                            break;

        Integer total = 0;
        for (int i = 0; i < config.NUM_ITEMS; i++) {
//        Integer i = start;
//        while (start < end) {

            success = false;
            int oldTotal = total;
            int oldI = i;
//            int oldStart = start;
            while (!success) {
//                start = oldStart;
                total = oldTotal;
                i = oldI;
//                reviewers = new ArrayList<Integer>();
                try {
                    client.startTransaction();
                    for (int j = 0; j < config.REVIEW_INCR; j++) {
                        List<Integer> reviewers = new ArrayList<Integer>();
                        int review_count = numReviews.nextValue();
                        if (review_count == 0)
                            review_count = 1;
                        for (int rc = 0; rc < review_count; ) {
                            //                        if (i >= end)
                            //                            break;

                            int u_id = reviewer.nextValue();
                            if (!reviewers.contains(u_id)) {
                                rc++;
                                row = reviewTable.createNewRow(config.PAD_COLUMNS);
                                reviewTable.updateColumn("R_A_ID", total, row);
                                reviewTable.updateColumn("R_U_ID", u_id, row);
                                reviewTable.updateColumn("R_I_ID", i, row);
                                reviewTable.updateColumn("R_RATING", new Random().nextInt(5), row);
                                reviewTable.updateColumn("R_RANK", 0, row);
                                client.write(EpinionsConstants.kReviewTable, total.toString(), row);
                                reviewers.add(u_id);
                                total++;


                                //                            if (start == 0 && rc == 0) {
                                //                                row = reviewByIIDTable.createNewRow(config.PAD_COLUMNS);
                                //                                reviewByIIDTable.updateColumn("ID_LIST",
                                //                                        new shield.benchmarks.epinions.SerializableIDSet(config).serialize(), row);
                                //                                client.write(EpinionsConstants.kReviewByIIDTable, Integer.toString(u_id), row);
                                //                                row = reviewByUIDTable.createNewRow(config.PAD_COLUMNS);
                                //                                reviewByUIDTable.updateColumn("ID_LIST",
                                //                                        new shield.benchmarks.epinions.SerializableIDSet(config).serialize(), row);
                                //                                client.write(EpinionsConstants.kReviewByUIDTable, Integer.toString(i), row);
                                //                            }

                                addToIndexList(client, config, reviewByUIDTable, Integer.toString(u_id), total);
                                addToIndexList(client, config, reviewByIIDTable, Integer.toString(i), total);
                            }
                        }
                        System.out.println(review_count + "/" + config.NUM_ITEMS + " Review Start: " + i);
                        i++;
                    }
//                    start++;
                    client.commitTransaction();
                    success = true;
                } catch (DatabaseAbortException e) {
                    System.err.println("Trx Aborted ");
                    success = false;
                } catch (Exception e) {
                    e.printStackTrace();
                    System.err.println(e.getMessage());
                    success = false;
                    System.err.println("Retrying");
                }
            }
            System.out.print("Loaded " + (total - oldTotal) + " rows for users " + i + "\n");
        }
        System.out.println("Loaded " + total + " total");

//        client.requestExecutor.shutdown();
//        while (!client.requestExecutor.isTerminated()) {}
    }

    /* Adds to key to value mapping or updates key's list of values in table */
    private static void addToIndexList(ClientBase client, EpinionsExperimentConfiguration config,
                                       Table table, String key, Integer value) throws DatabaseAbortException {
        List<byte[]> results = client.readAndExecute(table.getTableName(), key);
        byte[] valueRow;
        boolean isNull = false;
        if (results != null) {
//            System.out.println("NOTTTT null");
            valueRow = results.get(0);
        } else {
//            System.out.println("null");
            isNull = true;
            valueRow = table.createNewRow(config.PAD_COLUMNS);
        }

        shield.benchmarks.epinions.SerializableIDSet values;
        // check if key is already in the table
        if (isRowEmpty(valueRow) || isNull) {
//            System.out.println("first");
            valueRow = table.createNewRow(config.PAD_COLUMNS);
            values = new shield.benchmarks.epinions.SerializableIDSet(config);
        } else {
            String strValList = (String) table.getColumn("ID_LIST", valueRow);
            values = new SerializableIDSet(config, strValList);
        }
        values.add(value);
//        System.out.println("values" + values.serialize().length());
//        System.out.println(valueRow.length);
        table.updateColumn("ID_LIST", values.serialize(), valueRow);
        client.write(table.getTableName(), key, valueRow);
//        System.out.println("test");
    }

    private static boolean isRowEmpty(byte[] row) {
        return row.length == 0;
    }

    private static void loadTrust(int start, int end, int port, String expConfigFile, AtomicLong count)
            throws InterruptedException, ParseException, IOException, DatabaseAbortException, SQLException {

        EpinionsExperimentConfiguration config = new EpinionsExperimentConfiguration(expConfigFile);
        ClientBase client = ClientUtils.createClient(config.CLIENT_TYPE,expConfigFile, port, port);

        System.out.println("Loading Trust");

        client.registerClient();
        EpinionsGenerator generator = new EpinionsGenerator(client, config);

        Table trustTable = client.getTable(EpinionsConstants.kTrustTable);
        Integer custId;

        ZipfianIntGenerator numTrust = new ZipfianIntGenerator(config.TRUST, 1.95);
        ScrambledZipfianGenerator reviewed = new ScrambledZipfianGenerator(config.NUM_USERS);
        Random isTrusted = new Random(System.currentTimeMillis());

        boolean success;
        byte[] row;

        Integer total = 0;
//        for (int i = 0; i < config.NUM_USERS; i++) {
        Integer i = start;
        while (start < end) {
            List<Integer> trusted = new ArrayList<Integer>();
            int trust_count = numTrust.nextValue();
            success = false;
            int oldTotal = total;
            int oldI = i;
            int oldStart = start;
            while (!success) {
                total = oldTotal;
                i = oldI;
                start = oldStart;
                trusted = new ArrayList<Integer>();
                try {
                    client.startTransaction();
                    if (i >= end)
                        break;
                    for (int tc = 0; tc < trust_count;) {
                        int u_id = reviewed.nextValue().intValue();
                        if (!trusted.contains(u_id)) {
                            tc++;
                            row = trustTable.createNewRow(config.PAD_COLUMNS);
                            trustTable.updateColumn("T_SU_ID", i, row);
                            trustTable.updateColumn("T_TU_ID", u_id, row);
                            trustTable.updateColumn("T_TRUST", isTrusted.nextInt(2), row);
                            trustTable.updateColumn("T_DATE", i, row);
                            client.write(EpinionsConstants.kTrustTable, Integer.toString(i), row);
                            trusted.add(u_id);
                            total++;
                        }
                        System.out.println(count.incrementAndGet() * tc + "/" + config.NUM_ITEMS + " Trust Start: " + start);
                    }
                    i++;
                    start++;
                    client.commitTransaction();
                    success = true;
                } catch (DatabaseAbortException e) {
                    System.err.println("Trx Aborted ");
                    success = false;
                } catch (Exception e) {
                    e.printStackTrace();
                    System.err.println(e.getMessage());
                    success = false;
                    System.err.println("Retrying");
                }
            }
//            System.out.print("Loaded " + (total - oldTotal) + " rows for users " + i + "\n");
        }
        System.out.println("Loaded " + total + " trust");
    }

    private static void loadTrustSeq(int port, String expConfigFile, AtomicLong count)
            throws InterruptedException, ParseException, IOException, DatabaseAbortException, SQLException {

        EpinionsExperimentConfiguration config = new EpinionsExperimentConfiguration(expConfigFile);
        ClientBase client = ClientUtils.createClient(config.CLIENT_TYPE,expConfigFile, port, port);

        System.out.println("Loading Trust");

        client.registerClient();
        EpinionsGenerator generator = new EpinionsGenerator(client, config);

        Table trustTable = client.getTable(EpinionsConstants.kTrustTable);
        Integer custId;

        ZipfianIntGenerator numTrust = new ZipfianIntGenerator(config.TRUST, 1.95);
        ScrambledZipfianGenerator reviewed = new ScrambledZipfianGenerator(config.NUM_USERS);
        Random isTrusted = new Random(System.currentTimeMillis());

        boolean success;
        byte[] row;

        Integer total = 0;
        for (int i = 0; i < config.NUM_USERS; i++) {
//        Integer i = start;
//        while (start < end) {

            success = false;
            int oldTotal = total;
            int oldI = i;
//            int oldStart = start;
            while (!success) {
                total = oldTotal;
                i = oldI;
//                start = oldStart;
//                trusted = new ArrayList<Integer>();
                try {
                    client.startTransaction();
//                    if (i >= end)
//                        break;
                    for (int j = 0; j < 20; j++) {
                        List<Integer> trusted = new ArrayList<Integer>();
                        int trust_count = numTrust.nextValue();
                        for (int tc = 0; tc < trust_count; ) {
                            int u_id = reviewed.nextValue().intValue();
                            if (!trusted.contains(u_id)) {
                                tc++;
                                row = trustTable.createNewRow(config.PAD_COLUMNS);
                                trustTable.updateColumn("T_SU_ID", i, row);
                                trustTable.updateColumn("T_TU_ID", u_id, row);
                                trustTable.updateColumn("T_TRUST", isTrusted.nextInt(2), row);
                                trustTable.updateColumn("T_DATE", i, row);
                                client.write(EpinionsConstants.kTrustTable, Integer.toString(i), row);
                                trusted.add(u_id);
                                total++;
                            }

                        }
                        System.out.println(count.incrementAndGet() * trust_count + "/" + config.NUM_ITEMS + " Trust Start: " + i);
                        i++;
                    }
//                    i += 20;
//                    start++;
                    client.commitTransaction();
                    success = true;
                } catch (DatabaseAbortException e) {
                    System.err.println("Trx Aborted ");
                    success = false;
                } catch (Exception e) {
                    e.printStackTrace();
                    System.err.println(e.getMessage());
                    success = false;
                    System.err.println("Retrying");
                }
            }
//            System.out.print("Loaded " + (total - oldTotal) + " rows for users " + i + "\n");
        }
        System.out.println("Loaded " + total + " trust");
    }
}
