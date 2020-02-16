## **Playing With Hbase-Testing-Utility**

HBase comes with a useful class called **HBaseTestingUtility**, which makes it easy to write integration tests using a mini-cluster. The first step is to add the below dependency to your Maven POM file.  
Use the version of hbase per your needs.


    <dependency>
        <groupId>org.apache.hbase</groupId>
        <artifactId>hbase-testing-util</artifactId>
        <version>${hbase.version}</version>
        <scope>test</scope>
    </dependency>

Below is an example, how the hbase minicluster could be set up for testing

```
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
```
Once the minicluster is started, it's availibility could be verified by using the below-

```
HBaseAdmin.available(utility.getConfiguration());
```

Next, hbase connection object can be created as below:

```
@Test
    public void testGetConnection() throws IOException {
        Connection connection = null;
        try {
            connection = hbaseOperations.getHBaseConnection(HBaseConfiguration.create(utility.getConfiguration()));
            assertEquals(false, connection.isClosed());
            connection.close();

        } catch (IOException e) {
            if(connection != null){
                if (!connection.isClosed())
                    connection.close();
            }
            e.printStackTrace();
        }
    }
```

All other operations can be carried out using the connection object and admin object.
Further examples are present in HbaseOperationsTest.java class.