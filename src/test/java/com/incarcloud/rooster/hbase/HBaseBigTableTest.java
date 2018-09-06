package com.incarcloud.rooster.hbase;

import com.incarcloud.rooster.bigtable.IBigTable;
import com.incarcloud.rooster.datapack.DataPackTrip;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.junit.*;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Properties;

/**
 * HbaseBigTableTest
 *
 * @author Aaric, created on 2017-12-11T14:58.
 * @since 1.0-SNAPSHOT
 */
public class HBaseBigTableTest {

    // HBase连接参数
    public static final String HBASE_ZK_QUORUM = "10.0.11.34,10.0.11.35,10.0.11.39";
    public static final String HBASE_Zk_PORT = "2181";
    public static final String HBASE_MASTER = "10.0.11.35:60000";

    // HBase数据表和列名
    private static final String TABLE_NAME_VEHICLE = "gmmc:vehicle";
    private static final String TABLE_NAME_TELEMETRY = "gmmc:telemetry";
    private static final String COLUMN_FAMILY_NAME = "base";

    private IBigTable bigTable;
    private Connection connection;

    @Before
    public void begin() throws Exception {
        // Properties
        Properties props = new Properties();
        props.put("hbase.zookeeper.quorum", HBASE_ZK_QUORUM);
        props.put("hbase.zookeeper.property.clientPort", HBASE_Zk_PORT);
        props.put("hbase.master", HBASE_MASTER);
        bigTable = new HBaseBigTable(props, TABLE_NAME_TELEMETRY, TABLE_NAME_VEHICLE);

        // Connection
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", HBASE_ZK_QUORUM);
        configuration.set("hbase.zookeeper.property.clientPort", HBASE_Zk_PORT);
        configuration.set("hbase.master", HBASE_MASTER);
        connection = ConnectionFactory.createConnection(configuration);
    }

    @After
    public void end() throws IOException {
        if (null != bigTable) {
            bigTable.close();
        }
        if (null != connection) {
            connection.close();
        }
    }

    @Test
    @Ignore
    public void testQueryDataList() throws Exception {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date startTime = dateFormat.parse("2017-12-15 11:46:45");
        Date endTime = dateFormat.parse("2017-12-15 11:47:05");
        List<DataPackTrip> dataList = bigTable.queryData("LB370X1Z0GJ051724", DataPackTrip.class, startTime, endTime);
        if (null != dataList && 0 < dataList.size()) {
            dataList.forEach(object -> System.out.println(object));
        }
        System.out.println(dataList);
        Assert.assertEquals(0, dataList.size());
    }

    @Ignore
    @Test(expected = Exception.class)
    public void createTable() throws IOException {
        // Admin
        Admin admin = connection.getAdmin();

        // create vehicle
        TableName tableName = TableName.valueOf(TABLE_NAME_VEHICLE);
        if (admin.tableExists(tableName)) {
            // delete
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }
        HTableDescriptor desc = new HTableDescriptor(tableName);
        admin.createTable(desc.addFamily(new HColumnDescriptor(COLUMN_FAMILY_NAME)));

        // create telemetry
        tableName = TableName.valueOf(TABLE_NAME_TELEMETRY);
        if (admin.tableExists(tableName)) {
            // delete
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }
        desc = new HTableDescriptor(tableName);
        admin.createTable(desc.addFamily(new HColumnDescriptor(COLUMN_FAMILY_NAME)));
    }

    @Test
    @Ignore
    public void testQueryDataByKey() throws Exception {
        int pageSize = 5;
        List<DataPackTrip> tripList1 = bigTable.queryData("LSBAAAAAAZZ000001", DataPackTrip.class, pageSize, null);
        tripList1.forEach(object -> System.out.println(object.getId()));
        Assert.assertEquals(pageSize, tripList1.size());
        List<DataPackTrip> tripList2 = bigTable.queryData("LSBAAAAAAZZ000001", DataPackTrip.class, pageSize, "bc3c000LSBAAAAAAZZ000001TRIP###########20180706120000####0001");
        tripList2.forEach(object -> System.out.println(object.getId()));
        Assert.assertEquals(pageSize - 1, tripList2.size());
    }

    @Test
    @Ignore
    public void testQueryTrips() {
        List<DataPackTrip> tripList = bigTable.queryData("LGWEEUK53HE000040", DataPackTrip.class, 5, null);
        tripList.forEach(object -> System.out.println(object));
        Assert.assertNotEquals(0, tripList.size());
    }
}
