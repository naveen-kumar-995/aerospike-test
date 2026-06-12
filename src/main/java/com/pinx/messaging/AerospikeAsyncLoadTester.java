
package com.pinx.messaging;

import com.aerospike.client.*;
import com.aerospike.client.async.EventLoop;
import com.aerospike.client.async.EventLoops;
import com.aerospike.client.async.EventPolicy;
import com.aerospike.client.async.NioEventLoops;
import com.aerospike.client.listener.RecordListener;
import com.aerospike.client.listener.WriteListener;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.WritePolicy;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.Record;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class AerospikeAsyncLoadTester {

    private static final String HOST = "10.10.2.44";
    private static final int PORT = 3000;
    private static final String NAMESPACE = "deliveries";
    private static final String SET = "dlr_success";

    private static final int INSERT_TPS = 10000;
    private static final int READ_TPS = 10000;

    private static final String KEY_FILE = "keys.txt";

    private static final AtomicLong insertSuccess = new AtomicLong();
    private static final AtomicLong insertFailure = new AtomicLong();
    private static final AtomicLong readSuccess = new AtomicLong();
    private static final AtomicLong readFailure = new AtomicLong();

    private static final BlockingQueue<String> keyQueue = new LinkedBlockingQueue<>(500000);

    private final AerospikeClient client;
    private final EventLoops eventLoops;
    private final WritePolicy writePolicy = new WritePolicy();

    public AerospikeAsyncLoadTester() throws IOException {
        ClientPolicy cp = new ClientPolicy();
        cp.failIfNotConnected = true;
        cp.maxConnsPerNode = 500;

        client = new AerospikeClient(cp, HOST, PORT);
        System.out.println("Connected=" + client.isConnected());
        EventPolicy eventPolicy = new EventPolicy();

        eventPolicy.minTimeout = 100;
        eventPolicy.maxCommandsInProcess = 100000;

        eventLoops = new NioEventLoops(
                eventPolicy,
                Runtime.getRuntime().availableProcessors()
        );
        writePolicy.expiration = 172800;
        writePolicy.sendKey = true;
    }

    private static final String PAYLOAD =
            "{\"car_ts_format\":\"yyMMddHHmmss\",\"dlt_tmpl_id\":\"1107177029299157846\",\"sms_max_rty_atmpt\":\"3\",\"s_rate\":\"0.0\",\"app_type\":\"sms\",\"recv_dt\":\"2026-06-08\",\"tz\":\"Asia\\\\/Kolkata\",\"intf_type\":\"http_japi\",\"su_add_fixed_rate\":\"0.02\",\"dest\":\"917338374687\",\"pu_add_fixed_rate\":\"0.025\",\"dn_ori_sts_code\":\"001\",\"rute_logic_id\":\"5\",\"su_id\":\"1500000000000000\",\"usr\":\"gen_banking_dltchk_n_TRANS\",\"car_full_dn\":\"id:DA86D4610CE0C0D23B0165A278EC0C43 sub:001 dlvrd:000 submit date:260608095640 done date:260608155641 stat:EXPIRED err:001 text:Received Rs.648.00 f\",\"car_sts_desc\":\"EXPIRED\",\"a_recv_ts\":\"2026-06-08 09:56:39.362\",\"m_id\":\"444301775639363435833800\",\"usr_ty\":\"2\",\"ft_cd\":\"PMM\",\"b_af_rate\":\"0.025\",\"pl_rds_id\":\"1\",\"rute_type\":\"SMPP\",\"dn_req_cli\":\"0\",\"car_dly_sts\":\"EXPIRED\",\"rty_atmpt\":\"0\",\"a_recv_dt\":\"2026-06-08\",\"r_af_rate\":\"0.025\",\"dlt_enty_id\":\"1701158055703165737\",\"car_ori_sts_desc\":\"EXPIRED\",\"platform_cluster\":\"trans\",\"d_hdr\":\"IDFCFB\",\"intf_grp_type\":\"api\",\"udh\":\"0500036e0201\",\"is_hex_m\":\"0\",\"msg_type\":\"1\",\"a_dly_ts\":\"2026-06-08 15:56:41.000\",\"f_id\":\"444301775639363425824000\",\"car_sys_id\":\"SKV_PIN_T10\",\"smsc_id\":\"AIR2\",\"sms_rety_avail\":\"3\",\"b_m_id\":\"444301775639363435833800\",\"bill_s_rate\":\"0.13\",\"from_comp\":\"dlr_receiver\",\"dly_ts\":\"2026-06-08 15:56:41.000\",\"car_rcvd_ts\":\"2026-06-08 09:56:40.000\",\"p6\":\"2e2f3b2282b040e2b99b01451627acbfT1780892799347\",\"r_sms_rate\":\"0.13\",\"intl_msg\":\"0\",\"p7\":\"6a26447f3932c826658b3887TKzkxNzMzODM3NDY4Nw\",\"nxt_comp\":\"dlr_processor\",\"cir\":\"Others\",\"bill_ty\":\"0\",\"su_sms_rate\":\"0.06\",\"d_rate\":\"0.0\",\"car_sub_ts\":\"2026-06-08 09:56:40.788\",\"sms_priority\":\"2\",\"a_car_sub_ts\":\"2026-06-08 09:56:40.788\",\"dcs\":\"-1\",\"a_rute_id\":\"AIR2\",\"car\":\"Others\",\"cntry\":\"India\",\"c_id\":\"1500000000030010\",\"cli_encp\":\"0\",\"treat_dom_as_spl_srs\":\"0\",\"pu_sms_rate\":\"0.13\",\"tot_m_prts\":\"2\",\"msg_create_ts\":\"1780892800787\",\"atmpt_cnt\":\"1\",\"udhi\":\"1\",\"bill_encp_type\":\"0\",\"pro_msg_ty\":\"DeliveryObject\",\"pl_exp\":\"26060815\",\"inv_b_on\":\"0\",\"dn_ori_sts_desc\":\"Unknown subscriber\",\"car_ack_id\":\"DA86D4610CE0C0D23B0165A278EC0C43\",\"m_prt_no\":\"1\",\"car_ori_sts_code\":\"001\",\"max_valid_in_sec\":\"86400\",\"m\":\"Received Rs.648.00 for loan#xxxxxxxxxxxx2169 and receipt JR08062621331515 on 08\\\\/06\\\\/2026. Total loan o\\\\/s-Rs.0.00 as of 08\\\\/06\\\\/2026. Details@180010888 IDFC\",\"dn_fail_type\":\"2\",\"dn_pl_sts\":\"1\",\"m_class\":\"PM\",\"car_sts_code\":\"001\",\"bill_af_rate\":\"0.025\",\"recv_ts\":\"2026-06-08 09:56:39.362\",\"dly_sts\":\"Failed\",\"pu_id\":\"1500000000030000\",\"hdr\":\"IDFCFB\",\"rute_id\":\"AIR2\",\"b_s_rate\":\"0.13\",\"is_wc\":\"0\"}','2026-06-08 15:56:41.000')";

    public void asyncInsert(String keyStr) {
        Key key = new Key(NAMESPACE, SET, keyStr);

        EventLoop loop = eventLoops.next();

        client.put(
                loop,
                new WriteListener() {
                    @Override
                    public void onSuccess(Key key) {
                        insertSuccess.incrementAndGet();
                        System.out.println("Submitting insert");
                        keyQueue.offer(key.userKey.toString());
                    }

                    @Override
                    public void onFailure(AerospikeException e) {
                        insertFailure.incrementAndGet();
                    }
                },
                writePolicy,
                key,
                new Bin("payload", PAYLOAD),
                new Bin("insert_ts", System.currentTimeMillis())
                  );
    }

    public void asyncRead(String keyStr) {

        EventLoop loop = eventLoops.next();

        client.get(
                loop,
                new RecordListener() {
                    @Override
                    public void onSuccess(Key key, com.aerospike.client.Record record) {
                        if (record != null) {
                            readSuccess.incrementAndGet();
                        }
                    }


                    @Override
                    public void onFailure(AerospikeException e) {
                        readFailure.incrementAndGet();
                    }
                },
                null,
                new Key(NAMESPACE, SET, keyStr)
                  );
    }

    private static void startMonitor() {
        Thread.ofVirtual().start(() -> {
            long prevInsert = 0;
            long prevRead = 0;

            while (true) {
                try {
                    Thread.sleep(1000);

                    long ins = insertSuccess.get();
                    long rd = readSuccess.get();

                    System.out.printf(
                            "[MONITOR] InsertTPS=%d ReadTPS=%d InsertFail=%d ReadFail=%d Queue=%d%n",
                            ins - prevInsert,
                            rd - prevRead,
                            insertFailure.get(),
                            readFailure.get(),
                            keyQueue.size()
                                     );

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

                StringBuilder buffer = new StringBuilder();

                while (true) {
                    String key = keyQueue.take();

                    buffer.append(key).append('\n');

                    if (buffer.length() > 65536) {
                        writer.write(buffer.toString());
                        writer.flush();
                        buffer.setLength(0);
                    }
                }
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    public static void main(String[] args) throws Exception {

        AerospikeAsyncLoadTester tester = new AerospikeAsyncLoadTester();

        startMonitor();
        startKeyWriter();

        ScheduledExecutorService scheduler =
                Executors.newScheduledThreadPool(2);

        scheduler.scheduleAtFixedRate(() -> {

            for (int i = 0; i < INSERT_TPS; i++) {

                String key =
                        UUID.randomUUID().toString() + "~0";

                tester.asyncInsert(key);
            }

        }, 0, 1, TimeUnit.SECONDS);

        scheduler.scheduleAtFixedRate(() -> {

            int count = 0;

            while (count < READ_TPS) {

                String key = keyQueue.poll();

                if (key == null) {
                    break;
                }

                tester.asyncRead(key);

                count++;
            }

        }, 0, 1, TimeUnit.SECONDS);

        Runtime.getRuntime().addShutdownHook(
                new Thread(() -> {
                    tester.client.close();
                    tester.eventLoops.close();
                    System.out.println("Shutdown complete.");
                })
                                            );

        Thread.currentThread().join();
    }
}
