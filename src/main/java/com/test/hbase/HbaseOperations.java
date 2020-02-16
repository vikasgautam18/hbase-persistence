package com.test.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.List;

/**
 *
 * A utility class to perform operations on Hbase
 *
 */
public class HbaseOperations {

    /**
     * Get a connectoin to connect to HBase from factory object
     * @param hbaseConfiguration
     * @return Connection object
     */
    public Connection getHBaseConnection(Configuration hbaseConfiguration){
        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection(hbaseConfiguration);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return connection;
    }

    /**
     * close a given connection
     * @param connection HBaseConnection Object
     */
    public void closeHBaseConnection(Connection connection) {
        try {
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Get an Admin object to perform administrative activities
     * @param connection HBaseConnection Object
     * @return Admin objecct
     * @throws IOException
     */
    public Admin getAdmin(Connection connection) throws IOException {
        return connection.getAdmin();
    }

    /**
     * A wrapper method to create an HBase table
     * @param connection
     * @param tableName
     * @param cf
     * @throws IOException
     */
    public void createTable(Connection connection, TableName tableName, String cf){
        try (Admin hbaseAdmin = getAdmin(connection)) {
            TableDescriptor desc = TableDescriptorBuilder.newBuilder(tableName)
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.of(cf))
                    .build();
            System.out.println("creating table " + tableName.toString() + " ...");
            hbaseAdmin.createTable(desc);
            System.out.println("table " + tableName.toString() + " created successfully!");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * A method to write data into an HBase table
     * @param connection
     * @param puts
     * @param tableName
     * @throws IOException
     */
    public void writeToTable(Connection connection, List<Put> puts, TableName tableName){
        try {
            Table table = connection.getTable(tableName);
            table.put(puts);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
