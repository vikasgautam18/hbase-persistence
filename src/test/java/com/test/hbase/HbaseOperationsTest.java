package com.test.hbase;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class HbaseOperationsTest {

    private static HBaseTestingUtility utility;
    private static HbaseOperations hbaseOperations;

    @BeforeClass
    public static void setUp() throws Exception {
        System.out.println("*** Setting up Hbase testing utility ***");
        utility = new HBaseTestingUtility();
        hbaseOperations = new HbaseOperations();

        try {
            System.out.println("*** Starting HBase Mini Cluster ***");
            utility.startMiniCluster();

            HBaseAdmin.available(utility.getConfiguration());
            System.out.println("*** HBase Mini Cluster Successfully started ***");
        } catch (Exception e) {
            if(utility != null){
                utility.shutdownMiniCluster();
            }
            e.printStackTrace();
        }
    }

    @AfterClass
    public static void tearDown() throws Exception {
        System.out.println("*** Shutting down Hbase Mini Cluster ***");
        utility.shutdownMiniCluster();
    }

    @Test
    public void testGetConnection() throws IOException {
        Connection connection = null;
        try {
            connection = hbaseOperations.getHBaseConnection(HBaseConfiguration.create(utility.getConfiguration()));
            assertFalse(connection.isClosed());
            connection.close();

        } catch (IOException e) {
            if (!connection.isClosed())
                connection.close();
            e.printStackTrace();
        }
    }

    @Test
    public void testCloseHBaseConnection(){
        Connection connection = hbaseOperations.getHBaseConnection(HBaseConfiguration.create(utility.getConfiguration()));
        hbaseOperations.closeHBaseConnection(connection);

        assertTrue(connection.isClosed());
    }

    @Test
    public void testGetAdmin() throws IOException {
        Connection connection = null;
        try {
             connection = hbaseOperations.getHBaseConnection(HBaseConfiguration.create(utility.getConfiguration()));
            Admin admin = hbaseOperations.getAdmin(connection);

            assertFalse(admin.isAborted());
            assertEquals("org.apache.hadoop.hbase.client.HBaseAdmin", admin.getClass().getCanonicalName());

        } catch (IOException e) {
            if (!connection.isClosed())
                connection.close();
            e.printStackTrace();
        }
    }

    @Test
    public void testCreateTable() throws IOException {
        Connection connection = null;
        try {
            connection = hbaseOperations.getHBaseConnection(HBaseConfiguration.create(utility.getConfiguration()));
            TableName NEW_TABLE = TableName.valueOf("test_table");
            byte[] COLUMN_FAMILY = Bytes.toBytes("cf");

            hbaseOperations.createTable(connection, NEW_TABLE , "cf");

            assertTrue(utility.getAdmin().tableExists(TableName.valueOf("test_table")));
            assertTrue(connection.getTable(NEW_TABLE).getDescriptor().getColumnFamilyNames().contains(COLUMN_FAMILY));
        } catch (IOException e) {
            if(connection != null){
                if (!connection.isClosed())
                    connection.close();
            }
            e.printStackTrace();
        }
    }

    @Test
    public void testWriteToTable() throws IOException {
        Connection connection = hbaseOperations.getHBaseConnection(HBaseConfiguration.create(utility.getConfiguration()));
        TableName NEW_TABLE = TableName.valueOf("test_table_one");
        byte[] COLUMN_FAMILY = Bytes.toBytes("cf");

        hbaseOperations.createTable(connection, NEW_TABLE , "cf");

        List<Put> puts = new ArrayList<>(10);

        for (int row = 0; row != 2; ++row) {
            byte[] bs = Bytes.toBytes(row);
            Put put = new Put(bs);
            put.addColumn(COLUMN_FAMILY, bs, bs);
            puts.add(put);
        }

        hbaseOperations.writeToTable(connection, puts, NEW_TABLE);

        Get get0 = new Get(Bytes.toBytes(0));
        Get get1 = new Get(Bytes.toBytes(1));

        Scan scan = new Scan();
        scan.addFamily(COLUMN_FAMILY);
        ResultScanner rs = connection.getTable(NEW_TABLE).getScanner(scan);

        int count = 0;
        for (Result result : rs) {
            System.out.println("result contains value :: " + Bytes.toInt(result.value()));
            count++;
        }

        assertEquals(2, count);
        assertEquals(0, Bytes.toInt(connection.getTable(NEW_TABLE).get(get0).value()));
        assertEquals(1, Bytes.toInt(connection.getTable(NEW_TABLE).get(get1).value()));
    }
}