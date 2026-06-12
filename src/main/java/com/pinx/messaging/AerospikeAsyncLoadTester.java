
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
            "{\"status\":\"DELIVRD\",\"cluster\":\"trans\",\"source\":\"loadtest\"}";

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
