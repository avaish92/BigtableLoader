package com.vaishya.aman;

/*
* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * */

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.util.GenericOptionsParser;
import java.io.IOException;



public class CreateTable {

    final static Log LOG = LogFactory.getLog(CreateTable.class);

    public static void createtable(TableName tableName, Configuration config,String colFam) throws IOException{

        Admin admin = null;
        Connection con = ConnectionFactory.createConnection();
        LOG.info("Checking of the table already exists, creating only if the table doesn't exists");
        try {
            admin = con.getAdmin();
            if (checkIfExists(tableName,admin)) LOG.info("Table Exists, Starting to Load Data");
            else {

                HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
                tableDescriptor.addFamily(new HColumnDescriptor(colFam));
                admin.createTable(tableDescriptor);
            }
        }
        catch (Exception e){
            LOG.error("Couldn't Create the table Exception" + tableName);
        }
        finally {
            admin.close();
            con.close();
        }

    }

    public static boolean checkIfExists(TableName tableName,Admin admin){
        try {
            return admin.tableExists(tableName);
        }catch (Exception e){
            LOG.fatal("Table Not found Exception" + tableName+ "Attempting to Create the table",e);
            return false;
        }

    }
}
