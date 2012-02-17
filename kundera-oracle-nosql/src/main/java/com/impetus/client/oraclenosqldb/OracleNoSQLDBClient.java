package com.impetus.client.oraclenosqldb;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.persistence.PersistenceException;

import oracle.kv.Direction;
import oracle.kv.KVStore;
import oracle.kv.Key;
import oracle.kv.KeyValueVersion;
import oracle.kv.Value;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.client.BaseClient;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.db.RelationHolder;
import com.impetus.kundera.index.IndexManager;
import com.impetus.kundera.metadata.model.Column;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.handler.impl.EntitySaveGraph;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessorHelper;

// TODO: Auto-generated Javadoc
/**
 * The Class OracleNoSQLDBClient.
 */
public class OracleNoSQLDBClient extends BaseClient implements Client {

    /** The is connected. */
    // private boolean isConnected;

    /** The kvstore db. */
    private KVStore kvStore;

    /** The log. */
    private static Log log = LogFactory.getLog(OracleNoSQLDBClient.class);

    /**
     * Instantiates a new oracle no sqldb client.
     * 
     * @param kvStore
     *            the kv store
     * @param indexManager
     *            the index manager
     * @param reader
     *            the reader
     */
    public OracleNoSQLDBClient(KVStore kvStore, IndexManager indexManager, EntityReader reader) {
        this.kvStore = kvStore;
        setIndexManager(indexManager);
        setReader(reader);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#find(java.lang.Class,
     * com.impetus.kundera.metadata.model.EntityMetadata, java.lang.Object,
     * java.util.List)
     */
    @Override
    public Object find(Class<?> clazz, EntityMetadata entityMetadata, Object key, List<String> relationNames) {

        log.debug("Fetching data from " + entityMetadata.getTableName() + " for PK " + key);

        List<String> majorComponents = new ArrayList<String>();
        majorComponents.add(entityMetadata.getTableName());
        majorComponents.add(key.toString());
        Key findKey = Key.createKey(majorComponents);
        Object entity = null;
        try {
            entity = entityMetadata.getEntityClazz().newInstance();
            PropertyAccessorHelper.setId(entity, entityMetadata, key.toString());
            Iterator<KeyValueVersion> i = kvStore.multiGetIterator(Direction.FORWARD, 0, findKey, null, null);

            while (i.hasNext()) {
                KeyValueVersion keyValueVersion = i.next();
                String columnName = keyValueVersion.getKey().getMinorPath().get(0);

                Value v = keyValueVersion.getValue();
                PropertyAccessorHelper.set(entity, entityMetadata.getColumn(columnName).getField(), v.getValue());
                // Do some work with the Value here

            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return entity;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#close()
     */
    @Override
    public void close() {
        // TODO Once pool is implemented this code should not be there.
        // Workaround for pool
        getIndexManager().flush();

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#delete(java.lang.Object,
     * java.lang.Object, com.impetus.kundera.metadata.model.EntityMetadata)
     */
    @Override
    public void delete(Object entity, Object pKey, EntityMetadata entityMetadata) throws Exception {

        Key key = Key.createKey(pKey.toString());
        kvStore.delete(key);
        getIndexManager().remove(entityMetadata, entity, pKey.toString());

    }

    

    /**
     * On persist.
     * 
     * @param entityMetadata
     *            the entity metadata
     * @param entity
     *            the entity
     * @param id
     *            the id
     * @param relations
     *            the relations
     * @throws Exception
     *             the exception
     * @throws PropertyAccessException
     *             the property access exception
     */
    protected void onPersist(EntityMetadata entityMetadata, Object entity, String id, List<RelationHolder> relations) throws Exception, PropertyAccessException {
        String dbName = entityMetadata.getSchema();
        String entityName = entityMetadata.getTableName();

        log.debug("Persisting data into " + dbName + "." + entityName + " for " + id);
        Key key = null;
        byte[] valueString = null;
        Value value = null;
        for (String columnName : entityMetadata.getColumnsMap().keySet()) {
            Column column = entityMetadata.getColumnsMap().get(columnName);
            List<String> majorKeyComponent = new ArrayList<String>();
            majorKeyComponent.add(entityName);
            majorKeyComponent.add(id);
            valueString = PropertyAccessorHelper.get(entity, column.getField());
            key = Key.createKey(majorKeyComponent, columnName);
            value = Value.createValue(valueString);
            kvStore.put(key, value);
        }

    }

  

  
    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#persistJoinTable(java.lang.String,
     * java.lang.String, java.lang.String,
     * com.impetus.kundera.metadata.model.EntityMetadata, java.lang.Object,
     * java.lang.Object)
     */
    @Override
    public void persistJoinTable(String joinTableName, String joinColumnName, String inverseJoinColumnName, EntityMetadata relMetadata, Object primaryKey, Object childEntity) {
        // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.client.Client#getForeignKeysFromJoinTable(java.lang
     * .String, java.lang.String, java.lang.String,
     * com.impetus.kundera.metadata.model.EntityMetadata,
     * com.impetus.kundera.persistence.handler.impl.EntitySaveGraph)
     */
    @Override
    public <E> List<E> getForeignKeysFromJoinTable(String joinTableName, String joinColumnName, String inverseJoinColumnName, EntityMetadata relMetadata, EntitySaveGraph objectGraph) {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.client.Client#deleteFromJoinTable(java.lang.String,
     * java.lang.String, java.lang.String,
     * com.impetus.kundera.metadata.model.EntityMetadata,
     * com.impetus.kundera.persistence.handler.impl.EntitySaveGraph)
     */
    @Override
    public void deleteFromJoinTable(String joinTableName, String joinColumnName, String inverseJoinColumnName, EntityMetadata relMetadata, EntitySaveGraph objectGraph) {
        // TODO Auto-generated method stub

    }

}
