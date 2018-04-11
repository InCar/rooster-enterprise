package com.incarcloud.rooster.hbase;

import com.incarcloud.rooster.bigtable.IBigTable;
import com.incarcloud.rooster.datapack.DataPackTrip;
import com.incarcloud.rooster.util.RowKeyUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Properties;

/**
 * HbaseBigTableTest
 *
 * @author Aaric, created on 2017-12-11T14:58.
 * @since 1.0-SNAPSHOT
 */
public class HbaseBigTableTest {

    private static final String TABLE_NAME_VEHICLE = "vehicle";
    private static final String TABLE_NAME_SECOND_INDEX = "second_index";
    private static final String TABLE_NAME_telemetry = "telemetry";
    private static final String COLUMN_FAMILY_NAME = "base";
    private static final String COLUMN_NAME_DATA = "data";

    public static final String HBASE_ZK_QUORUM = "10.0.11.34,10.0.11.35,10.0.11.39";
    public static final String HBASE_Zk_PORT = "2181";
    public static final String HBASE_MASTER = "10.0.11.35:60000";

    private IBigTable bigTable;

    @Before
    public void begin() throws Exception {
        Properties props = new Properties();
        props.put("hbase.zookeeper.quorum", HBASE_ZK_QUORUM);
        props.put("hbase.zookeeper.property.clientPort", HBASE_Zk_PORT);
        props.put("hbase.master", HBASE_MASTER);
        bigTable = new HbaseBigTable(props);
    }

    @After
    public void end() {
        if (null != bigTable) {
            bigTable.close();
        }
    }

    @Test
    @Ignore
    public void testQueryData() throws Exception {
        String startRowKey = RowKeyUtil.makeMinDetectionTimeIndexRowKey("20171215114547");
        String stopRowKey = RowKeyUtil.makeMaxDetectionTimeIndexRowKey("20171215181147");
        System.err.println(startRowKey);
        System.err.println(stopRowKey);
        String nextRowKey = bigTable.queryData(startRowKey, new IBigTable.IDataReadable() {

            @Override
            public void onRead(Object object) {
                System.out.println(object);
            }
        });
        System.err.println(nextRowKey);
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
    }

    @Test
    @Ignore
    public void testQueryLatestTimeMillis() throws Exception {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
        long millis = bigTable.queryLatestTimeMillis();
        if (0 != millis) {
            System.out.println(dateFormat.format(new Date(millis)));
        }
    }

    @Test
    @Ignore
    public void testQueryDataByQueryTime() throws Exception {
        DateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        Date queryTime = dateFormat.parse("20180411152256");
        bigTable.queryData(queryTime, object -> {
            System.out.println(object);
        });
    }

    @Test
    @Ignore
    public void createTable() throws IOException {
        // Connection
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", HBASE_ZK_QUORUM);
        configuration.set("hbase.zookeeper.property.clientPort", HBASE_Zk_PORT);
        configuration.set("hbase.master", HBASE_MASTER);
        Connection connection = ConnectionFactory.createConnection(configuration);

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

        // create second_index
        tableName = TableName.valueOf(TABLE_NAME_SECOND_INDEX);
        if (admin.tableExists(tableName)) {
            // delete
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }
        desc = new HTableDescriptor(tableName);
        admin.createTable(desc.addFamily(new HColumnDescriptor(COLUMN_FAMILY_NAME)));

        // create telemetry
        tableName = TableName.valueOf(TABLE_NAME_telemetry);
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
    public void testQueryMaxKey() throws Exception {
        // Connection
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", HBASE_ZK_QUORUM);
        configuration.set("hbase.zookeeper.property.clientPort", HBASE_Zk_PORT);
        configuration.set("hbase.master", HBASE_MASTER);
        Connection connection = ConnectionFactory.createConnection(configuration);

        // Filter
        Scan scan = new Scan();
        scan.setFilter(new KeyOnlyFilter());
        scan.setReversed(true);
        scan.setBatch(1);
        Table table = connection.getTable(TableName.valueOf(TABLE_NAME_SECOND_INDEX));

        // Query
        String maxRowKey = null;
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            maxRowKey = Bytes.toString(result.getRow());
            break;
        }

        // for long time
        if (StringUtils.isNotBlank(maxRowKey)) {
            String[] splitStrings = maxRowKey.split("_");
            if (null != splitStrings && 1 < splitStrings.length) {
                System.out.println(splitStrings[1]);
                DateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
                Calendar cal = Calendar.getInstance();
                cal.setTime(dateFormat.parse(splitStrings[1]));
                cal.set(Calendar.MILLISECOND, 0);
                System.out.println(cal.getTimeInMillis());
                System.out.println(dateFormat.format(new Date(cal.getTimeInMillis())));
            }
        }
    }
}
