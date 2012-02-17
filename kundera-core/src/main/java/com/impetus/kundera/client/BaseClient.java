package com.impetus.kundera.client;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.persistence.PersistenceException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.db.RelationHolder;
import com.impetus.kundera.index.IndexManager;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.handler.impl.EntitySaveGraph;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.proxy.EnhancedEntity;

public abstract class BaseClient implements Client{
    

    /** The index manager. */
    private IndexManager indexManager;

    /** The persistence unit. */
    private String persistenceUnit;

    /** The reader. */
    private EntityReader reader;

    /** The log. */
    private static Log log = LogFactory.getLog(BaseClient.class);
    
    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#persist(com.impetus.kundera.proxy.
     * EnhancedEntity)
     */
    @Override
    public void persist(EnhancedEntity e) throws Exception {
        log.error("persist method on enhance entity is not supported now!");
        throw new PersistenceException("method not supported");

    }
    

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#findAll(java.lang.Class,
     * java.lang.Object[])
     */
    @Override
    public <E> List<E> findAll(Class<E> entityClass, Object... keys) throws Exception {
        final List<E> list = new ArrayList<E>();
        for (Object key : keys) {
            list.add(find(entityClass, key, null));
        }
        return list;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#find(java.lang.Class,
     * java.util.Map)
     */
    @Override
    public <E> List<E> find(Class<E> entityClass, Map<String, String> embeddedColumnMap) throws Exception {
        return null;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#find(java.lang.Class,
     * java.lang.Object, java.util.List)
     */
    @Override
    public <E> E find(Class<E> entityClass, Object key, List<String> relationNames) throws Exception {
        final EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(getPersistenceUnit(), entityClass);
        return (E) find(entityClass, entityMetadata, key, relationNames);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#find(java.lang.String,
     * java.lang.String, com.impetus.kundera.metadata.model.EntityMetadata)
     */
    @Override
    public List<Object> find(String colName, String colValue, EntityMetadata m) {
        throw new UnsupportedOperationException("Method not supported");
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.client.Client#persist(com.impetus.kundera.persistence
     * .handler.impl.EntitySaveGraph,
     * com.impetus.kundera.metadata.model.EntityMetadata)
     */
    @Override
    public String persist(EntitySaveGraph entityGraph, EntityMetadata entityMetadata) {
        Object entity = entityGraph.getParentEntity();
        String id = entityGraph.getParentId();

        try {
            onPersist(entityMetadata, entity, id, RelationHolder.addRelation(entityGraph, entityGraph.getRevFKeyName(), entityGraph.getRevFKeyValue()));
            if (entityGraph.getRevParentClass() != null) {
                getIndexManager().write(entityMetadata, entity, entityGraph.getRevFKeyValue(), entityGraph.getRevParentClass());
            } else {
                getIndexManager().write(entityMetadata, entity);
            }
//        } catch (PropertyAccessException e) {
//            log.error(e.getMessage());
//            throw new PersistenceException(e.getMessage());
        } catch (Exception e) {
            log.error(e.getMessage());
            throw new PersistenceException(e.getMessage());
        }

        return null;

    }
    
    protected abstract void onPersist(EntityMetadata entityMetadata, Object entity, String id, List<RelationHolder> addRelation) throws Exception, PropertyAccessException ;

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#persist(java.lang.Object,
     * com.impetus.kundera.persistence.handler.impl.EntitySaveGraph,
     * com.impetus.kundera.metadata.model.EntityMetadata)
     */
    @Override
    public void persist(Object childEntity, EntitySaveGraph entitySaveGraph, EntityMetadata metadata) {
        String rlName = entitySaveGraph.getfKeyName();
        String rlValue = entitySaveGraph.getParentId();
        String id = entitySaveGraph.getChildId();

        try {
            onPersist(metadata, childEntity, id, RelationHolder.addRelation(entitySaveGraph, rlName, rlValue));
            onIndex(childEntity, entitySaveGraph, metadata, rlValue);
        } catch (PropertyAccessException e) {
            e.printStackTrace();
            log.error(e.getMessage());
            throw new PersistenceException(e.getMessage());
        } catch (Exception e) {
            log.error(e.getMessage());
            throw new PersistenceException(e.getMessage());
        }

    }
    
    /**
     * On index.
     * 
     * @param childEntity
     *            the child entity
     * @param entitySaveGraph
     *            the entity save graph
     * @param metadata
     *            the metadata
     * @param rlValue
     *            the rl value
     */
    protected void onIndex(Object childEntity, EntitySaveGraph entitySaveGraph, EntityMetadata metadata, String rlValue) {
        if (!entitySaveGraph.isSharedPrimaryKey()) {
            getIndexManager().write(metadata, childEntity, rlValue, entitySaveGraph.getParentEntity().getClass());
        } else {
            getIndexManager().write(metadata, childEntity);
        }
    }

    public IndexManager getIndexManager() {
        return indexManager;
    }


    public void setIndexManager(IndexManager indexManager) {
        this.indexManager = indexManager;
    }


    public String getPersistenceUnit() {
        return persistenceUnit;
    }


    public void setPersistenceUnit(String persistenceUnit) {
        this.persistenceUnit = persistenceUnit;
    }


    public EntityReader getReader() {
        return reader;
    }


    public void setReader(EntityReader reader) {
        this.reader = reader;
    }
}
