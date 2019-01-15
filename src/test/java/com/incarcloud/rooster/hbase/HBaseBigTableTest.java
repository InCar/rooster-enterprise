package com.incarcloud.rooster.hbase;

import com.incarcloud.rooster.bigtable.IBigTable;
import com.incarcloud.rooster.datapack.*;
import com.incarcloud.rooster.util.DataPackObjectUtil;
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

    /**
     * HBase连接参数
     */
    public static final String HBASE_ZK_QUORUM = "10.0.11.34,10.0.11.35,10.0.11.39";
    public static final String HBASE_Zk_PORT = "2181";
    public static final String HBASE_MASTER = "10.0.11.35:60000";

    /**
     * HBase数据表和列名
     */
    private static final String TABLE_NAME_VEHICLE = "gmmc:vehicle";
    private static final String TABLE_NAME_TELEMETRY = "gmmc:telemetry";
    private static final String COLUMN_FAMILY_NAME = "base";

    /**
     * 操作对象
     */
    private HBaseBigTable bigTable;
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
    public void testCreateTable() throws Exception {
        String tableName = "zdmp:telemetry";
        Connection connection = bigTable.getConnection();
        Admin admin = connection.getAdmin();
        if (!admin.tableExists(TableName.valueOf(tableName))) {
            HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
            HColumnDescriptor family = new HColumnDescriptor("cf1");
            family.setBlockCacheEnabled(true);
            family.setInMemory(true);
            family.setMaxVersions(1);
            family.setTimeToLive(3 * 365 * (1000 * 60 * 60 * 24));  // 3 years
            desc.addFamily(family);

            admin.createTable(desc);
        }
    }

    @Test
    @Ignore
    public void testGetData() {
        DataPackAlarm dataPackAlarm = bigTable.getData("bc3c000LSBAAAAAAZZ000001ALARM##########20180910151538####0001", DataPackAlarm.class);
        System.out.println(dataPackAlarm);
        Assert.assertNotNull(dataPackAlarm);
    }

    @Test
    @Ignore
    public void testGetDataLatest() {
        // 查询最早一条行程记录
        DataPackTrip dataPackTrip = bigTable.getData("LSBAAAAAAZZ000001", DataPackTrip.class, IBigTable.Sort.ASC);
        System.out.println(dataPackTrip.getId());
        Assert.assertEquals("bc3c000LSBAAAAAAZZ000001TRIP###########20180901120000####0001", dataPackTrip.getId());

        // 查询最近一条行程记录
        dataPackTrip = bigTable.getData("LSBAAAAAAZZ000001", DataPackTrip.class, IBigTable.Sort.DESC);
        System.out.println(dataPackTrip.getId());
        Assert.assertEquals("bc3c000LSBAAAAAAZZ000001TRIP###########20180910120000####0001", dataPackTrip.getId());
    }

    @Test
    @Ignore
    public void testQueryDataPackPositionList() throws Exception {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date startTime = dateFormat.parse("2018-09-10 10:00:00");
        Date endTime = dateFormat.parse("2018-09-10 12:00:00");
        List<DataPackPosition> dataList = bigTable.queryData("LSBAAAAAAZZ000001", DataPackPosition.class, startTime, endTime);
        if (null != dataList && 0 < dataList.size()) {
            dataList.forEach(object -> System.out.println(object.getId()));
        }
        System.out.println(dataList);
        Assert.assertNotEquals(0, dataList.size());
    }

    @Test
    @Ignore
    public void testQueryDataWithPage() {
        int pageSize = 5;
        List<DataPackTrip> tripList1 = bigTable.queryData("LSBAAAAAAZZ000001", DataPackTrip.class, pageSize, null);
        tripList1.forEach(object -> System.out.println(object.getId()));
        Assert.assertEquals(pageSize, tripList1.size());
        System.out.println("--");
        List<DataPackTrip> tripList2 = bigTable.queryData("LSBAAAAAAZZ000001", DataPackTrip.class, pageSize, "bc3c000LSBAAAAAAZZ000001TRIP###########20180906120000####0001");
        tripList2.forEach(object -> System.out.println(object.getId()));
        Assert.assertEquals(pageSize, tripList2.size());
    }

    @Test
    @Ignore
    public void testQueryDataWithPageByComplex() throws Exception {
        DateFormat dateFormat = new SimpleDateFormat(DataPackObjectUtil.DATE_PATTERN);
        Date startTime = dateFormat.parse("20180902120000");
        Date endTime = dateFormat.parse("20180905120000");
        List<DataPackTrip> tripList = bigTable.queryData("LSBAAAAAAZZ000001", DataPackTrip.class, IBigTable.Sort.ASC, startTime, endTime, 5, null);
        tripList.forEach(object -> System.out.println(object.getId()));
        Assert.assertNotEquals(0, tripList.size());
    }

    @Test
//    @Ignore
    public void testQueryTrips() {
        List<DataPackTrip> tripList = bigTable.queryData("LSBAAAAAAZZ000008", DataPackTrip.class, 5, null);
        tripList.forEach(object -> System.out.println(object.getId()));
        Assert.assertNotEquals(0, tripList.size());
    }

    @Test
    @Ignore
    public void testQueryActivation() {
        DataPackActivation activationObject = bigTable.getData("LGWEEUK53HE000051", DataPackActivation.class, IBigTable.Sort.ASC);
        System.out.println(activationObject.getId());
        Assert.assertNotNull(activationObject);
    }

    @Test
    @Ignore
    public void testQueryLogin() {
        String vin = "LGWEEUK53HE000051";
        DataPackLogIn logIn = bigTable.getData(vin, DataPackLogIn.class, IBigTable.Sort.DESC);
        System.out.println(logIn);
        System.out.println(logIn.getSoftwareVersion());
        Assert.assertNotNull(logIn);
    }

    @Test
//    @Ignore
    public void testQueryOverviewByRowKey() {
        //String rowKey = "LGWEEUK53HE000051";
        String rowKey = "1319000LGWEEUK53HE000051OVERVIEW#######20180913155316####0001";
        DataPackOverview overview = bigTable.getData(rowKey, DataPackOverview.class);
        System.out.println(overview);
    }

    @Test
    @Ignore
    public void testPageSizeIssue() {
        List<DataPackOverview> dataPackOverviewList = bigTable.queryData("LL66HAB06HB050029", DataPackOverview.class, 1, null);
        dataPackOverviewList.forEach(object -> System.out.println(object.getId())); //record count -> 3

//        DataPackOverview dataPackOverview = bigTable.getData("LL66HAB06HB050029", DataPackOverview.class, IBigTable.Sort.DESC);
//        System.out.println(dataPackOverview);
    }
}
