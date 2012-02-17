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
package com.impetus.client.hbase;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.PersistenceException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTablePool;

import com.impetus.client.hbase.admin.DataHandler;
import com.impetus.client.hbase.admin.HBaseDataHandler;
import com.impetus.kundera.Constants;
import com.impetus.kundera.client.BaseClient;
import com.impetus.kundera.db.RelationHolder;
import com.impetus.kundera.index.IndexManager;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.MetadataUtils;
import com.impetus.kundera.metadata.model.Column;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.handler.impl.EntitySaveGraph;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessorHelper;


/**
 * HBase client.
 * 
 * @author impetus
 */
public class HBaseClient extends BaseClient implements com.impetus.kundera.client.Client
{
    /** the log used by this class. */
    private static Log log = LogFactory.getLog(HBaseClient.class);

    /** The handler. */
    private DataHandler handler;


    /**
     * Instantiates a new h base client.
     *
     * @param indexManager the index manager
     * @param conf the conf
     * @param hTablePool the h table pool
     * @param reader the reader
     */
    public HBaseClient(IndexManager indexManager, HBaseConfiguration conf, HTablePool hTablePool, EntityReader reader)
    {
        setIndexManager(indexManager);
        this.handler = new HBaseDataHandler(conf, hTablePool);
        setReader(reader);
    }


    /* (non-Javadoc)
     * @see com.impetus.kundera.client.Client#find(java.lang.Class, java.lang.Object, java.util.List)
     */
    @SuppressWarnings("unchecked")
    @Override
    public <E> E find(Class<E> entityClass, Object rowId, List<String> relationNames) throws Exception
    {
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(getPersistenceUnit(), entityClass);
        // columnFamily has a different meaning for HBase, so it won't be used
        // here
        String tableName = entityMetadata.getTableName();
        Object enhancedEntity = handler.readData(tableName, entityMetadata.getEntityClazz(), entityMetadata,
                rowId != null ? rowId.toString() : null, relationNames);
        return (E) enhancedEntity;
    }

    /* (non-Javadoc)
     * @see com.impetus.kundera.client.Client#findAll(java.lang.Class, java.lang.Object[])
     */
    @SuppressWarnings("unchecked")
    @Override
    public <E> List<E> findAll(Class<E> entityClass, Object... rowIds) throws Exception
    {
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(getPersistenceUnit(), entityClass);
        List<E> entities = new ArrayList<E>();
        for (Object rowKey : rowIds)
        {
            E e = (E) handler.readData(entityMetadata.getTableName(), entityMetadata.getEntityClazz(), entityMetadata,
                    rowKey.toString(), null);
            entities.add(e);
        }
        return entities;
    }

    /* (non-Javadoc)
     * @see com.impetus.kundera.client.Client#find(java.lang.Class, java.util.Map)
     */
    @SuppressWarnings("unchecked")
    @Override
    public <E> List<E> find(Class<E> entityClass, Map<String, String> col) throws Exception
    {
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(getPersistenceUnit(), entityClass);
        List<E> entities = new ArrayList<E>();
        Map<String, Field> columnFamilyNameToFieldMap = MetadataUtils.createSuperColumnsFieldMap(entityMetadata);
        for (String columnFamilyName : col.keySet())
        {
            String entityId = col.get(columnFamilyName);
            E e = (E) handler.readData(entityMetadata.getTableName(), entityMetadata.getEntityClazz(), entityMetadata,
                    entityId, null);

            Field columnFamilyField = columnFamilyNameToFieldMap.get(columnFamilyName.substring(0,
                    columnFamilyName.indexOf("|")));
            Object columnFamilyValue = PropertyAccessorHelper.getObject(e, columnFamilyField);
            if (Collection.class.isAssignableFrom(columnFamilyField.getType()))
            {
                entities.addAll((Collection<E>) columnFamilyValue);
            }
            else
            {
                entities.add((E) columnFamilyValue);
            }
        }
        return entities;
    }

    /* (non-Javadoc)
     * @see com.impetus.kundera.client.Client#close()
     */
    @Override
    public void close()
    {
        handler.shutdown();

    }

   


   

    /* (non-Javadoc)
     * @see com.impetus.kundera.client.Client#persist(java.lang.Object, com.impetus.kundera.persistence.handler.impl.EntitySaveGraph, com.impetus.kundera.metadata.model.EntityMetadata)
     */
    @Override
    public void persist(Object childEntity, EntitySaveGraph entitySaveGraph, EntityMetadata entityMetadata)
    {
        String rlName = entitySaveGraph.getfKeyName();
        String rlValue = entitySaveGraph.getParentId();
        String id = entitySaveGraph.getChildId();
        onPersist(entityMetadata, childEntity, id, RelationHolder.addRelation(entitySaveGraph, rlName, rlValue));
        onIndex(childEntity, entitySaveGraph, entityMetadata, rlValue);
    }

    /**
     * On persist.
     *
     * @param entityMetadata the entity metadata
     * @param entity the entity
     * @param id the id
     * @param relations the relations
     */
    protected void onPersist(EntityMetadata entityMetadata, Object entity, String id, List<RelationHolder> relations)
    {
        String dbName = entityMetadata.getSchema(); // Has no meaning for HBase,
                                                    // not used
        String tableName = entityMetadata.getTableName();

        List<String> columnFamilyNames = new ArrayList<String>();

        // If this entity has columns(apart from embedded objects, they will be
        // treated as column family)
        List<Column> columns = entityMetadata.getColumnsAsList();
        if (columns != null && !columns.isEmpty())
        {
            columnFamilyNames.addAll(entityMetadata.getColumnFieldNames());
        }

        // Check whether this table exists, if not create it
        columnFamilyNames.addAll(entityMetadata.getEmbeddedColumnFieldNames());

        // Add relationship fields if they are there
        if (relations != null)
        {
            for (RelationHolder rh : relations)
            {
                columnFamilyNames.add(rh.getRelationName());
            }
        }

        try
        {
            handler.createTableIfDoesNotExist(tableName, columnFamilyNames.toArray(new String[0]));

            // Write data to HBase

            handler.writeData(tableName, entityMetadata, entity, id, relations);
        }
        catch (IOException e)
        {
            throw new PersistenceException(e.getMessage());
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#persistJoinTable(java.lang.String,
     * java.lang.String, java.lang.String,
     * com.impetus.kundera.persistence.handler.impl.EntitySaveGraph)
     */
    @Override
    public void persistJoinTable(String joinTableName, String joinColumnName, String inverseJoinColumnName,

    EntityMetadata relMetadata, Object primaryKey, Object childEntity)

    {
        String parentId = (String) primaryKey;
        Map<String, String> columns = new HashMap<String, String>();
        try
        {
            if (Collection.class.isAssignableFrom(childEntity.getClass()))
            {
                Collection<?> children = (Collection<?>) childEntity;

                for (Object child : children)
                {
                    String childId = PropertyAccessorHelper.getId(child, relMetadata);
                    columns.put(inverseJoinColumnName + "_" + childId, childId);
                }

            }
            else
            {
                String childId = PropertyAccessorHelper.getId(childEntity, relMetadata);
                columns.put(inverseJoinColumnName + "_" + childId, childId);
            }

            if (columns != null && !columns.isEmpty())
            {
                handler.createTableIfDoesNotExist(joinTableName, Constants.JOIN_COLUMNS_FAMILY_NAME);
                handler.writeJoinTableData(joinTableName, parentId, columns);
            }
        }
        catch (PropertyAccessException e)
        {
            e.printStackTrace();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }

    }

    /* (non-Javadoc)
     * @see com.impetus.kundera.client.Client#getForeignKeysFromJoinTable(java.lang.String, java.lang.String, java.lang.String, com.impetus.kundera.metadata.model.EntityMetadata, com.impetus.kundera.persistence.handler.impl.EntitySaveGraph)
     */
    @Override
    public <E> List<E> getForeignKeysFromJoinTable(String joinTableName, String joinColumnName,
            String inverseJoinColumnName, EntityMetadata relMetadata, EntitySaveGraph objectGraph)
    {
        String parentId = objectGraph.getParentId();
        return handler.getForeignKeysFromJoinTable(joinTableName, parentId, inverseJoinColumnName);

    }

    /* (non-Javadoc)
     * @see com.impetus.kundera.client.Client#deleteFromJoinTable(java.lang.String, java.lang.String, java.lang.String, com.impetus.kundera.metadata.model.EntityMetadata, com.impetus.kundera.persistence.handler.impl.EntitySaveGraph)
     */
    @Override
    public void deleteFromJoinTable(String joinTableName, String joinColumnName, String inverseJoinColumnName,
            EntityMetadata relMetadata, EntitySaveGraph objectGraph)
    {
        String pKey = objectGraph.getParentId();
        try
        {
            handler.deleteRow(pKey.toString(), joinTableName);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

   
    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#find(java.lang.Class,
     * com.impetus.kundera.metadata.model.EntityMetadata, java.lang.String)
     */
    @Override
    public Object find(Class<?> clazz, EntityMetadata entityMetadata, Object rowId, List<String> relationNames)
    {
        String tableName = entityMetadata.getTableName();
        Object enhancedEntity = null;
        try
        {
            enhancedEntity = handler.readData(tableName, entityMetadata.getEntityClazz(), entityMetadata,
                    rowId != null ? rowId.toString() : null, relationNames);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        return enhancedEntity;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#delete(java.lang.Object,
     * java.lang.Object, com.impetus.kundera.metadata.model.EntityMetadata)
     */
    @Override
    public void delete(Object entity, Object pKey, EntityMetadata metadata) throws Exception
    {
        handler.deleteRow(pKey.toString(), metadata.getTableName());
    }



}
