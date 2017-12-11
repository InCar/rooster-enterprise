package com.incarcloud.rooster.hbase;

import com.incarcloud.rooster.bigtable.IBigTable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
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

    public static final String HBASE_ZK_QUORUM = "10.0.11.30,10.0.11.31,10.0.11.32";
    public static final String HBASE_Zk_PORT = "2181";
    public static final String HBASE_MASTER = "10.0.11.30:60000";

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
        // TODO queryData
    }

    @Test
    public void testQueryDataList() throws Exception {
        // TODO queryDataList
        System.out.println(bigTable);
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
}
