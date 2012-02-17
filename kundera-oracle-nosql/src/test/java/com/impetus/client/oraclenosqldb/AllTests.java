package com.impetus.client.oraclenosqldb;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.util.kvlite.KVLite;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({ BasicKVStoreTest.class, EntityPersistenceKVStoreTest.class })
public class AllTests {

    /** The kv lite. */
    protected static KVLite kvLite;

    @BeforeClass
    public static void setUpClass() {
        System.out.println("Master setup");
        String storeName = "kvstore";
        String hostName = "localhost";
        String hostPort = "5000";

        try {
            KVStoreFactory.getStore(new KVStoreConfig(storeName, hostName + ":" + hostPort));
        } catch (Exception e) {
            try {
                delete(new File("./kvroot"));
            } catch (IOException e3) {
                e3.printStackTrace();
            }
            try {
                delete(new File("./lucene"));
            } catch (IOException e3) {
                e3.printStackTrace();
            }
            // server not found running, try running it
            kvLite = new KVLite("./kvroot", "kvstore", 5000, 5001, "localhost", "5010,5020", 1, false);
            try {
                kvLite.start();
            } catch (Exception e2) {
                e2.printStackTrace();
            }
            waitForKVStoreToStart();
        }

    }

    @AfterClass
    public static void tearDownClass() {
        System.out.println("Master tearDown");
        if (kvLite != null) {
            try {
                Thread.sleep(1 * 1000);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            kvLite.shutdownStore(true);
        }
    }

    /**
     * Delete kvstore.
     * 
     * @param f
     *            the f
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    private static void delete(File f) throws IOException {
        if (f.isDirectory()) {
            for (File c : f.listFiles())
                delete(c);
        }
        if (!f.delete())
            throw new FileNotFoundException("Failed to delete file: " + f);
    }

    /**
     * Wait for kv store to start.
     */
    private static void waitForKVStoreToStart() {
        try {
            Thread.sleep(10 * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
