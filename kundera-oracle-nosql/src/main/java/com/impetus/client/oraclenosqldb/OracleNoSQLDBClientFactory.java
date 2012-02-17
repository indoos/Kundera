package com.impetus.client.oraclenosqldb;

import java.util.Properties;

import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.index.IndexManager;
import com.impetus.kundera.index.LuceneIndexer;
import com.impetus.kundera.loader.GenericClientFactory;
import com.impetus.kundera.metadata.MetadataUtils;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.impetus.kundera.persistence.EntityReader;

// TODO: Auto-generated Javadoc
/**
 * A factory for creating OracleNoSQLDBClient objects.
 */
public class OracleNoSQLDBClientFactory extends GenericClientFactory {

	/** The logger. */
	private static Logger logger = LoggerFactory
			.getLogger(OracleNoSQLDBClientFactory.class);

	/** The index manager. */
	IndexManager indexManager;

	/** The kvstore db. */
	private KVStore kvStore;

	/** The reader. */
	private EntityReader reader;

	/* (non-Javadoc)
	 * @see com.impetus.kundera.loader.Loader#unload(java.lang.String[])
	 */
	@Override
	public void unload(String... persistenceUnits) {
		indexManager.close();
		if (kvStore != null) {
			logger.info("Closing connection to kvStore.");
			kvStore.close();
			logger.info("Closed connection to kvStore.");
		} else {
			logger.warn("Can't close connection to kvStore, it was already disconnected");
		}

	}

	/* (non-Javadoc)
	 * @see com.impetus.kundera.loader.GenericClientFactory#initializeClient()
	 */
	@Override
	protected void initializeClient() {
		String luceneDirPath = MetadataUtils
				.getLuceneDirectory(getPersistenceUnit());
		indexManager = new IndexManager(LuceneIndexer.getInstance(
				new StandardAnalyzer(Version.LUCENE_34), luceneDirPath));
		reader = new OracleNoSQLDBEntityReader();

	}

	/* (non-Javadoc)
	 * @see com.impetus.kundera.loader.GenericClientFactory#createPoolOrConnection()
	 */
	@Override
	protected Object createPoolOrConnection() {
		kvStore = getConnection();
		return kvStore;
	}

	/**
	 * Gets the connection.
	 * 
	 * @return the connection
	 */
	private KVStore getConnection() {

		PersistenceUnitMetadata persistenceUnitMetadata = KunderaMetadata.INSTANCE
				.getApplicationMetadata().getPersistenceUnitMetadata(
						getPersistenceUnit());

		Properties props = persistenceUnitMetadata.getProperties();
		String hostName = (String) props
				.get(PersistenceProperties.KUNDERA_NODES);
		String defaultPort = (String) props
				.get(PersistenceProperties.KUNDERA_PORT);
		// keyspace is keystore
		String storeName = (String) props
				.get(PersistenceProperties.KUNDERA_KEYSPACE);
		String poolSize = props
				.getProperty(PersistenceProperties.KUNDERA_POOL_SIZE_MAX_ACTIVE);
		return KVStoreFactory.getStore(new KVStoreConfig(storeName, hostName
				+ ":" + defaultPort));
	}

	/* (non-Javadoc)
	 * @see com.impetus.kundera.loader.GenericClientFactory#instantiateClient()
	 */
	@Override
	protected Client instantiateClient() {
		// TODO Auto-generated method stub
		return new OracleNoSQLDBClient(kvStore, indexManager, reader);
	}

	/* (non-Javadoc)
	 * @see com.impetus.kundera.loader.GenericClientFactory#isClientThreadSafe()
	 */
	@Override
	protected boolean isClientThreadSafe() {

		return false;
	}

}
