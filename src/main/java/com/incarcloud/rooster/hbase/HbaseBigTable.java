package com.incarcloud.rooster.hbase;/**
 * Created by fanbeibei on 2017/7/10.
 */

import com.incarcloud.rooster.bigtable.IBigTable;
import com.incarcloud.rooster.datapack.DataPackObject;
import com.incarcloud.rooster.util.DataPackObjectUtils;
import com.incarcloud.rooster.util.HBaseUtil;
import com.incarcloud.rooster.util.RowKeyUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Properties;

/**
 * @author Fan Beibei
 * @Description: hbase 操作类，单例使用
 * @date 2017/7/10 18:04
 */
public class HbaseBigTable implements IBigTable {

    /**
     * Logger
     */
    private static Logger logger = LoggerFactory.getLogger(HbaseBigTable.class);

    /**
     * 保存vin码的表
     */
    private static final String TABLE_NAME_VEHICLE = "vehicle";

    /**
     * 二级索引表
     */
    private static final String TABLE_NAME_SECOND_INDEX = "second_index";

    /**
     * 列族
     */
    private static final String COLUMN_FAMILY_NAME = "base";

    /**
     * 数据列
     */
    private static final String COLUMN_NAME_DATA = "data";

    /**
     * HBase连接，重量级且线程安全，建议单例
     */
    private Connection connection;

    public HbaseBigTable(Properties props) throws IOException {
        if (!validate(props)) {
            throw new IllegalArgumentException();
        }
        if (null == System.getenv("HADOOP_HOME")) {
            throw new IllegalArgumentException("environment variable 'HADOOP_HOME' is null!");
        }

        /* 创建HBase连接 */
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", props.getProperty("hbase.zookeeper.property.clientPort"));
        configuration.set("hbase.zookeeper.quorum", props.getProperty("hbase.zookeeper.quorum"));
        configuration.set("hbase.master", props.getProperty("hbase.master"));
        connection = ConnectionFactory.createConnection(configuration);

        /* 创建数据表 */
        // admin
        Admin admin = connection.getAdmin();

        // create vehicle
        HTableDescriptor desc;
        TableName tableName = TableName.valueOf(TABLE_NAME_VEHICLE);
        if (!admin.tableExists(tableName)) {
            desc = new HTableDescriptor(tableName);
            admin.createTable(desc.addFamily(new HColumnDescriptor(COLUMN_FAMILY_NAME)));
        }

        // create second_index
        tableName = TableName.valueOf(TABLE_NAME_SECOND_INDEX);
        if (!admin.tableExists(tableName)) {
            desc = new HTableDescriptor(tableName);
            admin.createTable(desc.addFamily(new HColumnDescriptor(COLUMN_FAMILY_NAME)));
        }

        // create telemetry
        tableName = TableName.valueOf(DataPackObjectUtils.getTableName("default"));
        if (!admin.tableExists(tableName)) {
            desc = new HTableDescriptor(tableName);
            admin.createTable(desc.addFamily(new HColumnDescriptor(COLUMN_FAMILY_NAME)));
        }
    }

    /**
     * 验证参数
     *
     * @param props
     * @return
     */
    protected boolean validate(Properties props) {
        if (null == props) {
            return false;
        }
        if (null == props.get("hbase.zookeeper.quorum")) {
            return false;
        }
        if (null == props.get("hbase.master")) {
            return false;
        }
        if (null == props.get("hbase.zookeeper.property.clientPort")) {
            return false;
        }
        return true;
    }

    @Override
    public void saveDataPackObject(String rowKey, DataPackObject data, Date recieveTime) throws Exception {
        /* 保存二级索引 */
        Table indexTable = connection.getTable(TableName.valueOf(TABLE_NAME_SECOND_INDEX));
        String secondIndexRowKey = RowKeyUtil.makeDetectionTimeIndexRowKey(DataPackObjectUtils.convertDetectionDateToString(data.getDetectionTime()), data.getVin(), DataPackObjectUtils.getDataType(data));

        // 一个PUT代表一行数据，再NEW一个PUT表示第二行数据,每行一个唯一的ROWKEY，此处rowkey为put构造方法中传入的值
        Put indexPut = new Put(secondIndexRowKey.getBytes());
        indexPut.addColumn(Bytes.toBytes(COLUMN_FAMILY_NAME), Bytes.toBytes(COLUMN_NAME_DATA), Bytes.toBytes(rowKey));
        indexTable.put(indexPut);

        /* 保存DataPack数据 */
        // Table对象线程不安全
        Table dataTable = connection.getTable(TableName.valueOf(DataPackObjectUtils.getTableName("default")));

        // 一个PUT代表一行数据，再NEW一个PUT表示第二行数据,每行一个唯一的ROWKEY，此处rowkey为put构造方法中传入的值
        Put dataPut = new Put(rowKey.getBytes());
        dataPut.addColumn(Bytes.toBytes(COLUMN_FAMILY_NAME), Bytes.toBytes(COLUMN_NAME_DATA), Bytes.toBytes(DataPackObjectUtils.toJson(data)));
        dataTable.put(dataPut);

        // TODO recieveTime 接收时间
        logger.debug("Save data pack object for vin({}) success!", rowKey);
    }

    @Override
    public void saveVin(String vin) throws Exception {
        // Table对象线程不安全
        Table dataTable = connection.getTable(TableName.valueOf(TABLE_NAME_VEHICLE));

        // 一个PUT代表一行数据，再NEW一个PUT表示第二行数据，每行一个唯一的ROWKEY，此处rowkey为put构造方法中传入的值
        Put dataPut = new Put(vin.getBytes());
        dataPut.addColumn(Bytes.toBytes(COLUMN_FAMILY_NAME), Bytes.toBytes(COLUMN_NAME_DATA), Bytes.toBytes(vin));
        dataTable.put(dataPut);

        logger.debug("Save vin({}) success.", vin);
    }

    @Override
    public String queryData(String startTimeRowKey, IDataReadable dataReadable) {
        try {
            // 根据开始row key和回调函数处理一批数据
            Table table = connection.getTable(TableName.valueOf(TABLE_NAME_SECOND_INDEX));

            // 构建查询条件
            Scan scan = new Scan();
            scan.setStartRow(Bytes.toBytes(""));
            scan.setStartRow(Bytes.toBytes(""));


            ResultScanner resultScanner = table.getScanner(scan);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public <T extends DataPackObject> List<T> queryData(String vinOrCode, Class<T> clazz, Date startTime, Date endTime) {
        // TODO 根据开始和结束时间查询数据
        return null;
    }

    @Override
    public void close() {
        if (null != connection) {
            try {
                connection.close();
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        }

    }
}
