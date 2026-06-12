package com.pinx.messaging;

import com.aerospike.client.*;
import com.aerospike.client.Record;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.WritePolicy;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class AerospikeSyncLoadTester {

    private static final Mode MODE = Mode.valueOf(getEnv("MODE", "MIXED").toUpperCase());
    private static final String HOST = getEnv("AEROSPIKE_HOST", "10.10.2.44");
    private static final int PORT = getIntEnv("AEROSPIKE_PORT", 3000);
    private static final String NAMESPACE = getEnv("AEROSPIKE_NAMESPACE", "deliveries");
    private static final String SET = getEnv("AEROSPIKE_SET", "dlr_success");
    private static final int INSERT_THREADS = getIntEnv("INSERT_THREADS", 3500);
    private static final int READ_THREADS = getIntEnv("READ_THREADS", 500);
    private static final String KEY_FILE = getEnv("KEY_FILE", "keys.txt");
    private static final int                   MAX_CONNS_PER_NODE = getIntEnv("MAX_CONNS_PER_NODE", 3000);
    private static final BlockingQueue<String> KEY_QUEUE          = new LinkedBlockingQueue<>(500000);
    private static final AtomicLong            INSERT_OK          = new AtomicLong();
    private static final AtomicLong            INSERT_FAIL        = new AtomicLong();
    private static final AtomicLong            READ_OK            = new AtomicLong();
    private static final AtomicLong            READ_FAIL          = new AtomicLong();
    private static final AtomicLong            INSERT_LATENCY     = new AtomicLong();
    private static final AtomicLong            READ_LATENCY       = new AtomicLong();
    private static final AtomicLong            READ_ATTEMPTED     = new AtomicLong();
    private static final AtomicLong            READ_FOUND         = new AtomicLong();
    private static final AtomicLong            READ_MISSED        = new AtomicLong();
    private static final AtomicLong            READ_FAILED        = new AtomicLong();
    private final        AerospikeClient       client;
    private final        WritePolicy           writePolicy;

    public AerospikeSyncLoadTester() {

        ClientPolicy cp = new ClientPolicy();
        cp.failIfNotConnected = true;
        cp.maxConnsPerNode = MAX_CONNS_PER_NODE;

        client = new AerospikeClient(cp, HOST, PORT);

        writePolicy = new WritePolicy();
        writePolicy.expiration = 172800;
        writePolicy.sendKey = true;

        System.out.println("Connected = " + client.isConnected());
    }

    private static String payload(String msgId) {
        return "{\"car_ts_format\":\"yyMMddHHmmss\",\"dlt_tmpl_id\":\"1107177029299157846\",\"sms_max_rty_atmpt\":\"3\",\"s_rate\":\"0.0\",\"app_type\":\"sms\",\"recv_dt\":\"2026-06-08\",\"tz\":\"Asia\\\\/Kolkata\",\"intf_type\":\"http_japi\",\"su_add_fixed_rate\":\"0.02\",\"dest\":\"917338374687\",\"pu_add_fixed_rate\":\"0.025\",\"dn_ori_sts_code\":\"001\",\"rute_logic_id\":\"5\",\"su_id\":\"1500000000000000\",\"usr\":\"gen_banking_dltchk_n_TRANS\",\"car_full_dn\":\"id:DA86D4610CE0C0D23B0165A278EC0C43 sub:001 dlvrd:000 submit date:260608095640 done date:260608155641 stat:EXPIRED err:001 text:Received Rs.648.00 f\",\"car_sts_desc\":\"EXPIRED\",\"a_recv_ts\":\"2026-06-08 09:56:39.362\",\"m_id\":\"444301775639363435833800\",\"usr_ty\":\"2\",\"ft_cd\":\"PMM\",\"b_af_rate\":\"0.025\",\"pl_rds_id\":\"1\",\"rute_type\":\"SMPP\",\"dn_req_cli\":\"0\",\"car_dly_sts\":\"EXPIRED\",\"rty_atmpt\":\"0\",\"a_recv_dt\":\"2026-06-08\",\"r_af_rate\":\"0.025\",\"dlt_enty_id\":\"1701158055703165737\",\"car_ori_sts_desc\":\"EXPIRED\",\"platform_cluster\":\"trans\",\"d_hdr\":\"IDFCFB\",\"intf_grp_type\":\"api\",\"udh\":\"0500036e0201\",\"is_hex_m\":\"0\",\"msg_type\":\"1\",\"a_dly_ts\":\"2026-06-08 15:56:41.000\",\"f_id\":\"444301775639363425824000\",\"car_sys_id\":\"SKV_PIN_T10\",\"smsc_id\":\"AIR2\",\"sms_rety_avail\":\"3\",\"b_m_id\":\"444301775639363435833800\",\"bill_s_rate\":\"0.13\",\"from_comp\":\"dlr_receiver\",\"dly_ts\":\"2026-06-08 15:56:41.000\",\"car_rcvd_ts\":\"2026-06-08 09:56:40.000\",\"p6\":\"2e2f3b2282b040e2b99b01451627acbfT1780892799347\",\"r_sms_rate\":\"0.13\",\"intl_msg\":\"0\",\"p7\":\"6a26447f3932c826658b3887TKzkxNzMzODM3NDY4Nw\",\"nxt_comp\":\"dlr_processor\",\"cir\":\"Others\",\"bill_ty\":\"0\",\"su_sms_rate\":\"0.06\",\"d_rate\":\"0.0\",\"car_sub_ts\":\"2026-06-08 09:56:40.788\",\"sms_priority\":\"2\",\"a_car_sub_ts\":\"2026-06-08 09:56:40.788\",\"dcs\":\"-1\",\"a_rute_id\":\"AIR2\",\"car\":\"Others\",\"cntry\":\"India\",\"c_id\":\"1500000000030010\",\"cli_encp\":\"0\",\"treat_dom_as_spl_srs\":\"0\",\"pu_sms_rate\":\"0.13\",\"tot_m_prts\":\"2\",\"msg_create_ts\":\"1780892800787\",\"atmpt_cnt\":\"1\",\"udhi\":\"1\",\"bill_encp_type\":\"0\",\"pro_msg_ty\":\"DeliveryObject\",\"pl_exp\":\"26060815\",\"inv_b_on\":\"0\",\"dn_ori_sts_desc\":\"Unknown subscriber\",\"car_ack_id\":\"DA86D4610CE0C0D23B0165A278EC0C43\",\"m_prt_no\":\"1\",\"car_ori_sts_code\":\"001\",\"max_valid_in_sec\":\"86400\",\"m\":\"Received Rs.648.00 for loan#xxxxxxxxxxxx2169 and receipt JR08062621331515 on 08\\\\/06\\\\/2026. Total loan o\\\\/s-Rs.0.00 as of 08\\\\/06\\\\/2026. Details@180010888 IDFC\",\"dn_fail_type\":\"2\",\"dn_pl_sts\":\"1\",\"m_class\":\"PM\",\"car_sts_code\":\"001\",\"bill_af_rate\":\"0.025\",\"recv_ts\":\"2026-06-08 09:56:39.362\",\"dly_sts\":\"Failed\",\"pu_id\":\"1500000000030000\",\"hdr\":\"IDFCFB\",\"rute_id\":\"AIR2\",\"b_s_rate\":\"0.13\",\"is_wc\":\"0\"}";   }

    private static void startMonitor() {

        Thread.ofVirtual().start(() -> {

            long prevInsert = 0;
            long prevRead = 0;

            while (true) {

                try {

                    Thread.sleep(1000);

                    long ins = INSERT_OK.get();
                    long rd = READ_FOUND.get();

                    long insTps = ins - prevInsert;
                    long rdTps = rd - prevRead;

                    double avgInsert = ins == 0 ? 0 : (INSERT_LATENCY.get() / 1_000_000.0) / ins;

                    double avgRead = rd == 0 ? 0 : (READ_LATENCY.get() / 1_000_000.0) / rd;

                    System.out.printf("[MONITOR] " + "InsertTPS=%d " + "ReadTPS=%d " + "Found=%d " + "Missed=%d " + "ReadFailed=%d " + "InsertAvg=%.3fms " + "ReadAvg=%.3fms " + "IF=%d " + "Queue=%d%n",

                            insTps, rdTps,

                            READ_FOUND.get(), READ_MISSED.get(), READ_FAILED.get(),

                            avgInsert, avgRead,

                            INSERT_FAIL.get(),

                            KEY_QUEUE.size());

                    prevInsert = ins;
                    prevRead = rd;

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

    private static void startKeyWriter() {

        Thread.ofVirtual().start(() -> {

            try (BufferedWriter writer = new BufferedWriter(new FileWriter(KEY_FILE, true))) {

                StringBuilder sb = new StringBuilder();

                while (true) {

                    String key = KEY_QUEUE.take();

                    sb.append(key).append('\n');

                    if (sb.length() > 65536) {
                        writer.write(sb.toString());
                        writer.flush();
                        sb.setLength(0);
                    }
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    public static void main(String[] args) throws Exception {

        AerospikeSyncLoadTester tester = new AerospikeSyncLoadTester();

        startMonitor();
        startKeyWriter();


        ExecutorService insertExecutor = Executors.newVirtualThreadPerTaskExecutor();

        ExecutorService readExecutor = Executors.newVirtualThreadPerTaskExecutor();

        /*
         * Start Insert Workers
         */
        if (MODE == Mode.INSERT || MODE == Mode.MIXED) {

            for (int i = 0; i < INSERT_THREADS; i++) {

                insertExecutor.submit(() -> {

                    while (true) {

                        try {

                            String key = UUID.randomUUID() + "~0";

                            tester.insert(key);

                            KEY_QUEUE.put(key);

                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                });
            }

            System.out.println("Started Insert Workers : " + INSERT_THREADS);
        }

        /*
         * Start Read Workers
         */
        if (MODE == Mode.READ || MODE == Mode.MIXED) {

            for (int i = 0; i < READ_THREADS; i++) {

                readExecutor.submit(() -> {

                    while (true) {

                        try {

                            String key = KEY_QUEUE.take();

                            tester.read(key);

                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                });
            }

            System.out.println("Started Read Workers : " + READ_THREADS);
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {

            System.out.println("Shutting down...");

            insertExecutor.shutdownNow();
            readExecutor.shutdownNow();

            tester.client.close();

            System.out.println("Shutdown complete");
        }));

        Thread.currentThread().join();
    }

    private static void sleepRemaining(long start) {

        long elapsed = System.nanoTime() - start;

        long sleepMs = 1000 - (elapsed / 1_000_000);

        if (sleepMs > 0) {
            try {
                Thread.sleep(sleepMs);
            } catch (Exception ignored) {
            }
        }
    }

    private static String getEnv(String key, String defaultValue) {
        return System.getenv().getOrDefault(key, defaultValue);
    }

    private static int getIntEnv(String key, int defaultValue) {
        try {
            return Integer.parseInt(System.getenv().getOrDefault(key, String.valueOf(defaultValue)));
        } catch (Exception e) {
            return defaultValue;
        }
    }

    private static long getLongEnv(String key, long defaultValue) {
        try {
            return Long.parseLong(System.getenv().getOrDefault(key, String.valueOf(defaultValue)));
        } catch (Exception e) {
            return defaultValue;
        }
    }

    public void insert(String keyStr) {

        long start = System.nanoTime();

        try {
            Key key = new Key(NAMESPACE, SET, keyStr);

            client.put(writePolicy, key, new Bin("payload", payload(keyStr)), new Bin("insert_ts", System.currentTimeMillis()));

            INSERT_OK.incrementAndGet();
            INSERT_LATENCY.addAndGet(System.nanoTime() - start);

            KEY_QUEUE.offer(keyStr);

        } catch (Exception e) {
            System.err.println("INSERT FAILED : " + e.getClass().getName());

            e.printStackTrace();

            INSERT_FAIL.incrementAndGet();
        }
    }

    public void read(String keyStr) {

        long start = System.nanoTime();

        READ_ATTEMPTED.incrementAndGet();

        try {

            Key key = new Key(NAMESPACE, SET, keyStr);

            Record record = client.get(null, key);

            if (record != null) {

                READ_FOUND.incrementAndGet();

            } else {

                READ_MISSED.incrementAndGet();
            }

            READ_LATENCY.addAndGet(System.nanoTime() - start);

        } catch (Exception e) {

            READ_FAILED.incrementAndGet();

            System.err.println("SELECT FAILED : " + e.getClass().getName());

            e.printStackTrace();
        }
    }

    enum Mode {INSERT, READ, MIXED}
}
