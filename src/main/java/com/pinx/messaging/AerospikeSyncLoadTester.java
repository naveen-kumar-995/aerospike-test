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
        return """
                {
                  "m_id":"%s",
                  "status":"DELIVRD",
                  "cluster":"trans",
                  "source":"loadtest"
                }
                """.formatted(msgId);
    }

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
