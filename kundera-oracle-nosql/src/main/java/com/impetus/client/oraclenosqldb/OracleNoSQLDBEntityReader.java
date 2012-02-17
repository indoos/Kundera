package com.impetus.client.oraclenosqldb;

import java.util.List;

import javax.persistence.PersistenceException;

import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.persistence.AbstractEntityReader;
import com.impetus.kundera.persistence.EntityReader;

// TODO: Auto-generated Javadoc
/**
 * The Class OracleNoSQLDBEntityReader.
 */
public class OracleNoSQLDBEntityReader extends AbstractEntityReader implements
		EntityReader {

	/* (non-Javadoc)
	 * @see com.impetus.kundera.persistence.EntityReader#populateRelation(com.impetus.kundera.metadata.model.EntityMetadata, java.util.List, boolean, com.impetus.kundera.client.Client)
	 */
	@Override
	public List<EnhanceEntity> populateRelation(EntityMetadata m,
			List<String> relationNames, boolean isParent, Client client) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.impetus.kundera.persistence.EntityReader#findById(java.lang.Object, com.impetus.kundera.metadata.model.EntityMetadata, java.util.List, com.impetus.kundera.client.Client)
	 */
	@Override
	public EnhanceEntity findById(Object primaryKey, EntityMetadata m,
			List<String> relationNames, Client client) {
	    try
        {
            Object o = client.find(m.getEntityClazz(), m, primaryKey, relationNames);
            if (o == null)
            {
                // No entity found
                return null;
            }
            else
            {
                return o instanceof EnhanceEntity ? (EnhanceEntity) o : new EnhanceEntity(o, getId(o, m), null);
            }

        }
        catch (Exception e)
        {
            e.printStackTrace();
            throw new PersistenceException(e.getMessage());
        }
	}

}
