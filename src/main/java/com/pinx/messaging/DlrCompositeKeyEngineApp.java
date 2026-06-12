package com.pinx.messaging;

import com.aerospike.client.*;
import com.aerospike.client.Record;
import com.aerospike.client.cdt.*;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.WritePolicy;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class DlrCompositeKeyEngineApp {

    // Infrastructure Mappings (Tuned to your 8-Core, 16GB RAM, V8.1.2.2 Docker config)
    private static final String HOST = "10.10.2.44";
    private static final int PORT = 3000;
    private static final String NAMESPACE = "deliveries";
    private static final String SET_NAME = "dlr_success";
    private static final String MAP_BIN = "splits";

    private final AerospikeClient client;
    private final WritePolicy writePolicy;

    // SERVER-SIDE TUNING: Directs Aerospike to sort splits automatically by key (msg_id/part number) on the raw SSD blocks
    private final MapPolicy orderedMapPolicy = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE);

    public DlrCompositeKeyEngineApp() {
        ClientPolicy policy = new ClientPolicy();
        this.client = new AerospikeClient(policy, HOST, PORT);

        this.writePolicy = new WritePolicy();
        this.writePolicy.expiration = 172800; // 48-Hour TTL fallback. Automated cleanup via host VM's NSUP
        this.writePolicy.sendKey = true;      // Retains key text inside the index for debugging transparency
    }

    /**
     * 1. ATOMIC INGESTION WORKER
     * Combines base_msg_id and retry_attempt into a single Composite Primary Key.
     * Appends arriving split payloads into the isolated composite row structure atomically.
     */
    public void insertSplitComposite(long cliId, String baseMsgId, int mPrtNo, String cluster, int retryAttempt, String fullJsonPayload) {
        // Construct the Composite Primary Key String: "444301775639363435833800~0"
        String compositeKeyString = baseMsgId + "~" + retryAttempt;
        Key key = new Key(NAMESPACE, SET_NAME, compositeKeyString);

        Bin binCli = new Bin("cli_id", cliId);
        Bin binCluster = new Bin("cluster", cluster);

        // Atomically update metadata and push the split fragment into the Key-Ordered Map
        client.operate(writePolicy, key,
                Operation.put(binCli),
                Operation.put(binCluster),
                MapOperation.put(orderedMapPolicy, MAP_BIN, Value.get(mPrtNo), Value.get(fullJsonPayload))
                      );
        System.out.printf("[Kafka Ingestion] Inserted Part #%d into Composite Row: %s%n", mPrtNo, compositeKeyString);
    }

    /**
     * 2. EXUIVALENT TO YOUR CLICKHOUSE SELECT QUERY
     * Selects payloads by reconstructing the composite key.
     * Returns them pre-sorted (equivalent to ORDER BY msg_id ASC) without any Java-side logic or indexing overhead.
     */
    public Collection<String> selectPayloadsOrdered(String baseMsgId, int retryAttempt) {
        // Reconstruct the exact same composite string to target the isolated row
        String compositeKeyString = baseMsgId + "~" + retryAttempt;
        Key key = new Key( NAMESPACE, SET_NAME, compositeKeyString);

        // Fetch ONLY the splits map bin from the targeted composite record row
        Record record = client.get(client.getReadPolicyDefault(), key, MAP_BIN);
        if (record == null) {
            return null; // Equivalent to 0 rows returned
        }

        @SuppressWarnings("unchecked")
        Map<Long, String> sortedSplits = (Map<Long, String>) record.getMap(MAP_BIN);
        if (sortedSplits == null || sortedSplits.isEmpty()) {
            return null;
        }

        // ORDER BY msg_id ASC
        // Because of MapOrder.KEY_ORDERED, calling values() guarantees sequence order (1 -> 2 -> 3...)
        return sortedSplits.values();
    }

    public void close() {
        if (this.client != null) {
            this.client.close();
        }
    }

    /**
     * Main Execution Harness simulating concurrent multi-retry streaming via Java 21 Virtual Threads
     */
    public static void main(String[] args) {
        DlrCompositeKeyEngineApp app = new DlrCompositeKeyEngineApp();

        String baseMsgIdStr = "444301775639363435833800";

        // Mock payloads representing different segments across original attempt (0) and retry attempt (1)
        String attempt0Part1 = "{\"m_prt_no\":\"1\",\"m\":\"[Attempt 0] Part 1 text \"}";
        String attempt0Part2 = "{\"m_prt_no\":\"2\",\"m\":\"[Attempt 0] Part 2 text.\"}";

        String attempt1Part1 = "{\"m_prt_no\":\"1\",\"m\":\"[Retry 1] Part 1 updated text \"}";
        String attempt1Part2 = "{\"m_prt_no\":\"2\",\"m\":\"[Retry 1] Part 2 updated text.\"}";

        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            System.out.println("--- Starting Composite Key Ingestion Stream ---");

            // Simulating simultaneous traffic processing for both Attempt 0 and Retry 1 streams
            executor.submit(() -> app.insertSplitComposite(1500000000030010L, baseMsgIdStr, 2, "trans", 0, attempt0Part2));
            executor.submit(() -> app.insertSplitComposite(1500000000030010L, baseMsgIdStr, 1, "trans", 1, attempt1Part1));
            executor.submit(() -> app.insertSplitComposite(1500000000030010L, baseMsgIdStr, 1, "trans", 0, attempt0Part1));
            executor.submit(() -> app.insertSplitComposite(1500000000030010L, baseMsgIdStr, 2, "trans", 1, attempt1Part2));

            // Wait briefly for the non-blocking I/O connection pools to finish streaming records down to the drive
            TimeUnit.MILLISECONDS.sleep(150);

            System.out.println("\n--- Simulating Separate Query Engine Executing SELECT Queries ---");

            // Direct Select for Attempt 0
            System.out.printf("Executing Select for ID: %s, Retry: 0%n", baseMsgIdStr);
            Collection<String> resultsAttempt0 = app.selectPayloadsOrdered(baseMsgIdStr, 0);
            if (resultsAttempt0 != null) {
                resultsAttempt0.forEach(payload -> System.out.println("  Selected Raw Row -> " + payload));
            }

            System.out.println();

            // Direct Select for Retry 1 (Perfect isolation, zero collision with attempt 0)
            System.out.printf("Executing Select for ID: %s, Retry: 1%n", baseMsgIdStr);
            Collection<String> resultsAttempt1 = app.selectPayloadsOrdered(baseMsgIdStr, 1);
            if (resultsAttempt1 != null) {
                resultsAttempt1.forEach(payload -> System.out.println("  Selected Raw Row -> " + payload));
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            System.out.println("\nShutting down connections.");
            app.close();
        }
    }
}