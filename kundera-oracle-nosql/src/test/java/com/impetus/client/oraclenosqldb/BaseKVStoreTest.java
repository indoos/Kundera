package com.impetus.client.oraclenosqldb;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import oracle.kv.FaultException;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.util.kvlite.KVLite;

import org.junit.After;
import org.junit.Before;

/**
 * Abstract Base class for all KVStoreTest.
 */
public abstract class BaseKVStoreTest {


    
    /** The store. */
    protected KVStore store;

    /**
     * Instantiates a new base kv store test.
     */
    public BaseKVStoreTest() {
        super();
    }

    /**
     * Setup.
     *
     * @throws InterruptedException the interrupted exception
     */
    @Before
    public void setup() throws InterruptedException {
        String storeName = "kvstore";
        String hostName = "localhost";
        String hostPort = "5000";

        try {
            store = KVStoreFactory.getStore(new KVStoreConfig(storeName, hostName + ":" + hostPort));
        } catch (Exception e) {
            //e.printStackTrace();
        }

    }

   

}