package com.test.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;

public class HBaseExample {

    public static HbaseOperations hbaseOperations = new HbaseOperations();

    public static void main(String[] args) {

        Configuration conf = HBaseConfiguration.create();
        Connection connection = hbaseOperations.getHBaseConnection(conf);
        TableName NEW_TABLE = TableName.valueOf("test_table");
        byte[] COLUMN_FAMILY = Bytes.toBytes("cf");
        int TBL_ROW_COUNT = 100;

        hbaseOperations.createTable(connection, NEW_TABLE , "cf");

        List<Put> puts = new ArrayList<>(TBL_ROW_COUNT);

        for (int row = 0; row != TBL_ROW_COUNT; ++row) {
            byte[] bs = Bytes.toBytes(row);
            Put put = new Put(bs);
            put.addColumn(COLUMN_FAMILY, bs, bs);
            puts.add(put);
        }
        hbaseOperations.writeToTable(connection, puts, NEW_TABLE);
        hbaseOperations.closeHBaseConnection(connection);
    }
}
