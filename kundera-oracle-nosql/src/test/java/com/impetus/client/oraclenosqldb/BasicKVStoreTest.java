package com.impetus.client.oraclenosqldb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeNotNull;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import oracle.kv.Direction;
import oracle.kv.KVStore;
import oracle.kv.Key;
import oracle.kv.KeyValueVersion;
import oracle.kv.Value;
import oracle.kv.ValueVersion;

import org.junit.Test;

public class BasicKVStoreTest extends BaseKVStoreTest {


    @Test
    public void testKVStore() {
        // fail("Not yet implemented");
        if (store==null) {
            System.err.println("Skippping integration test");
        }
        assumeNotNull(store);
        final String keyString = "Hello1";
        final String valueString = "Big Data World!";

        Key key = Key.createKey(keyString);
        Value value = Value.createValue(valueString.getBytes());
        store.put(Key.createKey(keyString), Value.createValue(valueString.getBytes()));
        ValueVersion valueVersion = store.get(Key.createKey(keyString));
        assertEquals(valueString, new String(valueVersion.getValue().getValue()));

        // store.delete(key);
        // store.put(key, value);

        List<String> majorKeyComponent = new ArrayList<String>();
        majorKeyComponent.add("PERSON");
        majorKeyComponent.add("1");
        Key findKey = Key.createKey(majorKeyComponent);
        String minorKeyComponent1 = "AGE";
        String expectedAge = "120";
        store.put(Key.createKey(majorKeyComponent, minorKeyComponent1), Value.createValue(expectedAge.getBytes()));
        String minorKeyComponent2 = "PERSON_NAME";
        String expectedName = "TESTUSER";

        store.put(Key.createKey(majorKeyComponent, minorKeyComponent2), Value.createValue(expectedName.getBytes()));

        Iterator<KeyValueVersion> i = store.multiGetIterator(Direction.FORWARD, 0, findKey, null, null);
        while (i.hasNext()) {
            KeyValueVersion keyValueVersion = i.next();
            String columnName = keyValueVersion.getKey().getMinorPath().get(0);
            Value v = keyValueVersion.getValue();
            if (columnName.equals(minorKeyComponent1)) {
                assertEquals(expectedAge, new String(v.getValue()));
            } else if (columnName.equals(minorKeyComponent2)) {
                assertEquals(expectedName, new String(v.getValue()));
            }
        }

        // /PERSON/1/-/AGE <Value: 49 48>
        // /PERSON/1/-/PERSON_NAME <Value: 118 105 118 101 107>
        // /PERSON/2/-/AGE <Value: 50 48>
        // /PERSON/2/-/PERSON_NAME <Value: 118 105 118 101 107>
        // /PERSON/3/-/AGE <Value: 49 53>
        // /PERSON/3/-/PERSON_NAME <Value: 118 105 118 101 107>
        //
        store.close();
    }

}
