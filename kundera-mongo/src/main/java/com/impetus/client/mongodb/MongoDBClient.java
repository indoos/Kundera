/*******************************************************************************
 * * Copyright 2011 Impetus Infotech.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 ******************************************************************************/
package com.impetus.client.mongodb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.client.BaseClient;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.db.RelationHolder;
import com.impetus.kundera.index.IndexManager;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.handler.impl.EntitySaveGraph;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

/**
 * CLient class for MongoDB database.
 * 
 * @author impetusopensource
 */
public class MongoDBClient extends BaseClient implements Client {

    /** The is connected. */
    // private boolean isConnected;

    /** The mongo db. */
    private DB mongoDb;

    /** The data handler. */
    // private MongoDBDataHandler dataHandler;

    /** The log. */
    private static Log log = LogFactory.getLog(MongoDBClient.class);

    /**
     * Instantiates a new mongo db client.
     * 
     * @param mongo
     *            the mongo
     * @param mgr
     *            the mgr
     * @param reader
     *            the reader
     */
    public MongoDBClient(Object mongo, IndexManager mgr, EntityReader reader) {
        // TODO: This could be a costly call, see how connection pooling is
        // relevant here
        this.mongoDb = (DB) mongo;
        setIndexManager(mgr);
        setReader(reader);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#persistJoinTable(java.lang.String,
     * java.lang.String, java.lang.String,
     * com.impetus.kundera.metadata.model.EntityMetadata,
     * com.impetus.kundera.persistence.handler.impl.EntitySaveGraph)
     */
    @Override
    public void persistJoinTable(String joinTableName, String joinColumnName, String inverseJoinColumnName, EntityMetadata relMetadata, Object primaryKey, Object childEntity) {
        DBCollection dbCollection = mongoDb.getCollection(joinTableName);

        List<BasicDBObject> documents = new ArrayList<BasicDBObject>();

        String parentId = (String) primaryKey;
        try {
            if (Collection.class.isAssignableFrom(childEntity.getClass())) {
                Collection children = (Collection) childEntity;

                for (Object child : children) {

                    addColumnsToJoinTable(joinColumnName, inverseJoinColumnName, relMetadata, documents, parentId, child);
                }

            } else

            {
                addColumnsToJoinTable(joinColumnName, inverseJoinColumnName, relMetadata, documents, parentId, childEntity);
            }
        } catch (PropertyAccessException e) {
            e.printStackTrace();
        }

        dbCollection.insert(documents.toArray(new BasicDBObject[0]));
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

        String parentId = objectGraph.getParentId();
        List<E> foreignKeys = new ArrayList<E>();

        DBCollection dbCollection = mongoDb.getCollection(joinTableName);
        BasicDBObject query = new BasicDBObject();

        query.put(joinColumnName, parentId);

        DBCursor cursor = dbCollection.find(query);
        DBObject fetchedDocument = null;

        while (cursor.hasNext()) {
            fetchedDocument = cursor.next();
            String foreignKey = (String) fetchedDocument.get(inverseJoinColumnName);
            foreignKeys.add((E) foreignKey);
        }
        return foreignKeys;
    }

    /**
     * Adds the columns to join table.
     * 
     * @param joinColumnName
     *            the join column name
     * @param inverseJoinColumnName
     *            the inverse join column name
     * @param relMetadata
     *            the rel metadata
     * @param documents
     *            the documents
     * @param parentId
     *            the parent id
     * @param child
     *            the child
     * @throws PropertyAccessException
     *             the property access exception
     */
    private void addColumnsToJoinTable(String joinColumnName, String inverseJoinColumnName, EntityMetadata relMetadata, List<BasicDBObject> documents, String parentId, Object child)
            throws PropertyAccessException {
        String childId = PropertyAccessorHelper.getId(child, relMetadata);
        BasicDBObject dbObj = new BasicDBObject();
        dbObj.put(joinColumnName, parentId);
        dbObj.put(inverseJoinColumnName, childId);

        documents.add(dbObj);
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
        String primaryKey = objectGraph.getParentId();
        DBCollection dbCollection = mongoDb.getCollection(joinTableName);

        /*
         * Set<String> childIds = new HashSet<String>(); Object childObject =
         * objectGraph.getChildEntity(); try {
         * if(Collection.class.isAssignableFrom(childObject.getClass())) {
         * for(Object child : (Collection)childObject) { if(child != null) {
         * String childId = PropertyAccessorHelper.getId(child, relMetadata);
         * childIds.add(childId); }
         * 
         * } } else { String childId = PropertyAccessorHelper.getId(childObject,
         * relMetadata); childIds.add(childId); } } catch
         * (PropertyAccessException e) { e.printStackTrace(); }
         * 
         * if(childIds.isEmpty() || primaryKey == null) { return; }
         */

        BasicDBObject query = new BasicDBObject();
        query.put(joinColumnName, primaryKey.toString());
        // query.put(inverseJoinColumnName, new BasicDBObject("$in",
        // childIds.toArray()));

        dbCollection.remove(query);
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
        String documentName = entityMetadata.getTableName();

        log.debug("Persisting data into " + dbName + "." + documentName + " for " + id);
        DBCollection dbCollection = mongoDb.getCollection(documentName);

        BasicDBObject document = new MongoDBDataHandler(this, getPersistenceUnit()).getDocumentFromEntity(entityMetadata, entity, relations);
        dbCollection.save(document);

    }

    /*
     * (non-Javadoc)
     * 
     * @seecom.impetus.kundera.Client#loadColumns(com.impetus.kundera.ejb.
     * EntityManager, java.lang.Class, java.lang.String, java.lang.String,
     * java.lang.String, com.impetus.kundera.metadata.EntityMetadata)
     */
    @Override
    public <E> E find(Class<E> entityClass, Object key, List<String> relationNames) throws Exception {
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(getPersistenceUnit(), entityClass);

        log.debug("Fetching data from " + entityMetadata.getTableName() + " for PK " + key);

        DBCollection dbCollection = mongoDb.getCollection(entityMetadata.getTableName());

        BasicDBObject query = new BasicDBObject();
        query.put(entityMetadata.getIdColumn().getName(), key.toString());

        DBCursor cursor = dbCollection.find(query);
        DBObject fetchedDocument = null;

        if (cursor.hasNext()) {
            fetchedDocument = cursor.next();
        } else {
            return null;
        }

        Object enhancedEntity = new MongoDBDataHandler(this, getPersistenceUnit()).getEntityFromDocument(entityMetadata.getEntityClazz(), entityMetadata, fetchedDocument, relationNames);

        return (E) enhancedEntity;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#findAll(java.lang.Class,
     * java.lang.Object[])
     */
    @Override
    public <E> List<E> findAll(Class<E> entityClass, Object... keys) throws Exception {
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(getPersistenceUnit(), entityClass);

        log.debug("Fetching data from " + entityMetadata.getTableName() + " for Keys " + keys);

        DBCollection dbCollection = mongoDb.getCollection(entityMetadata.getTableName());

        BasicDBObject query = new BasicDBObject();

        query.put(entityMetadata.getIdColumn().getName(), new BasicDBObject("$in", getString(keys)));

        DBCursor cursor = dbCollection.find(query);

        List entities = new ArrayList<E>();
        while (cursor.hasNext()) {
            DBObject fetchedDocument = cursor.next();
            Object entity = new MongoDBDataHandler(this, getPersistenceUnit()).getEntityFromDocument(entityMetadata.getEntityClazz(), entityMetadata, fetchedDocument, null);
            entities.add(entity);
        }
        return entities;
    }

    /**
     * Loads columns from multiple rows restricting results to conditions stored
     * in <code>filterClauseQueue</code>.
     * 
     * @param <E>
     *            the element type
     * @param entityMetadata
     *            the entity metadata
     * @param mongoQuery
     *            the mongo query
     * @param result
     *            the result
     * @param relationNames
     *            the relation names
     * @param orderBy
     *            the order by
     * @return the list
     * @throws Exception
     *             the exception
     */
    public <E> List<E> loadData(EntityMetadata entityMetadata, BasicDBObject mongoQuery, String result, List<String> relationNames, BasicDBObject orderBy) throws Exception {
        String documentName = entityMetadata.getTableName();
        String dbName = entityMetadata.getSchema();
        Class clazz = entityMetadata.getEntityClazz();

        DBCollection dbCollection = mongoDb.getCollection(documentName);
        List entities = new ArrayList<E>();

        // If User wants search on a column within a particular super column,
        // fetch that embedded object collection only
        // otherwise retrieve whole entity
        // TODO: improve code
        if (result.indexOf(".") >= 0) {
            // TODO i need to discuss with Amresh before modifying it.
            entities.addAll(new MongoDBDataHandler(this, getPersistenceUnit()).getEmbeddedObjectList(dbCollection, entityMetadata, documentName, mongoQuery, result, orderBy));

        } else {
            log.debug("Fetching data from " + documentName + " for Filter " + mongoQuery.toString());

            DBCursor cursor = orderBy != null ? dbCollection.find(mongoQuery).sort(orderBy) : dbCollection.find(mongoQuery);
            MongoDBDataHandler handler = new MongoDBDataHandler(this, getPersistenceUnit());
            while (cursor.hasNext()) {
                DBObject fetchedDocument = cursor.next();
                Object entity = handler.getEntityFromDocument(clazz, entityMetadata, fetchedDocument, relationNames);
                entities.add(entity);
            }
        }

        return entities;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#delete(java.lang.Object,
     * java.lang.Object, com.impetus.kundera.metadata.model.EntityMetadata)
     */
    @Override
    public void delete(Object entity, Object pKey, EntityMetadata entityMetadata) throws Exception {
        DBCollection dbCollection = mongoDb.getCollection(entityMetadata.getTableName());

        // Find the DBObject to remove first
        BasicDBObject query = new BasicDBObject();
        query.put(entityMetadata.getIdColumn().getName(), pKey.toString());

        dbCollection.remove(query);
        getIndexManager().remove(entityMetadata, entity, pKey.toString());

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

    /**
     * Creates the index.
     * 
     * @param collectionName
     *            the collection name
     * @param columnList
     *            the column list
     * @param order
     *            the order
     */
    public void createIndex(String collectionName, List<String> columnList, int order) {
        DBCollection coll = mongoDb.getCollection(collectionName);

        List<DBObject> indexes = coll.getIndexInfo(); // List of all current
        // indexes on collection
        Set<String> indexNames = new HashSet<String>(); // List of all current
        // index names
        for (DBObject index : indexes) {
            BasicDBObject obj = (BasicDBObject) index.get("key");
            Set<String> set = obj.keySet(); // Set containing index name which
            // is key
            indexNames.addAll(set);
        }

        // Create index if not already created
        for (String columnName : columnList) {
            if (!indexNames.contains(columnName)) {
                coll.createIndex(new BasicDBObject(columnName, order));
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#find(java.lang.Class,
     * com.impetus.kundera.metadata.model.EntityMetadata, java.lang.String)
     */
    @Override
    public Object find(Class<?> clazz, EntityMetadata entityMetadata, Object rowId, List<String> relationNames) {

        log.debug("Fetching data from " + entityMetadata.getTableName() + " for PK " + rowId);

        DBCollection dbCollection = mongoDb.getCollection(entityMetadata.getTableName());

        BasicDBObject query = new BasicDBObject();
        query.put(entityMetadata.getIdColumn().getName(), rowId.toString());

        DBCursor cursor = dbCollection.find(query);
        DBObject fetchedDocument = null;

        if (cursor.hasNext()) {
            fetchedDocument = cursor.next();
        } else {
            return null;
        }

        Object entity = new MongoDBDataHandler(this, getPersistenceUnit()).getEntityFromDocument(entityMetadata.getEntityClazz(), entityMetadata, fetchedDocument, relationNames);

        return entity;
    }

    /**
     * Method to find entity for given association name and association value.
     * 
     * @param colName
     *            the col name
     * @param colValue
     *            the col value
     * @param m
     *            the m
     * @return the list
     */
    public List<Object> find(String colName, String colValue, EntityMetadata m) {
        // you got column name and column value.
        DBCollection dbCollection = mongoDb.getCollection(m.getTableName());

        BasicDBObject query = new BasicDBObject();

        query.put(colName, colValue);

        DBCursor cursor = dbCollection.find(query);
        DBObject fetchedDocument = null;
        MongoDBDataHandler handler = new MongoDBDataHandler(this, getPersistenceUnit());
        List<Object> results = new ArrayList<Object>();
        while (cursor.hasNext()) {
            fetchedDocument = cursor.next();
            Object entity = handler.getEntityFromDocument(m.getEntityClazz(), m, fetchedDocument, null);
            results.add(entity);
        }

        return results.isEmpty() ? null : results;
    }

    /**
     * Gets the string.
     * 
     * @param pKeys
     *            the keys
     * @return the string
     */
    private String[] getString(Object[] pKeys) {

        if (pKeys != null) {
            String[] arr = new String[pKeys.length];
            int counter = 0;
            for (Object o : pKeys) {
                arr[counter++] = o.toString();
            }

            return arr;
        }

        return null;
    }

}
