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
package com.impetus.client.oraclenosqldb;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.client.Client;


// TODO: Auto-generated Javadoc
/**
 * Provides utility methods for handling data held in Oracle NoSQL KVstore.
 *
 * @author amresh.singh
 */
public class OracleNoSQLDBDataHandler
{
    
    /** The client. */
    private Client client;

    /** The persistence unit. */
    private String persistenceUnit;

    /**
     * Instantiates a new mongo db data handler.
     *
     * @param client the client
     * @param persistenceUnit the persistence unit
     */
    public OracleNoSQLDBDataHandler(Client client, String persistenceUnit)
    {
        super();
        this.client = client;
        this.persistenceUnit = persistenceUnit;
    }

    /** The log. */
    private static Log log = LogFactory.getLog(OracleNoSQLDBDataHandler.class);

    
}