package com.incarcloud.rooster.hbase;/**
 * Created by fanbeibei on 2017/7/10.
 */

import com.incarcloud.rooster.bigtable.IBigTable;
import com.incarcloud.rooster.datapack.DataPackObject;
import com.incarcloud.rooster.util.DataPackObjectUtils;
import com.incarcloud.rooster.util.RowKeyUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

/**
 * @author Fan Beibei
 * @Description: hbase 操作类，单例使用
 * @date 2017/7/10 18:04
 */
public class HbaseBigTable implements IBigTable {
    private static Logger s_logger = LoggerFactory.getLogger(HbaseBigTable.class);
    /**
     * 保存vin码的表
     */
    private static final String VIN_TABLE = "vehicle";
    /**
     * 二级索引表
     */
    private static final String SECOND_INDEX_TABLE = "second_index";

    /**
     * 列族
     */
    private static final String COLUMN_FAMILY = "base";
    /**
     * 数据列
     */
    private static final String COLUMN_DATA = "data";

    /**
     * hbase 连接，重量级且线程安全，建议单例
     */
    private Connection connection;

    public HbaseBigTable(Properties props) throws IOException {
        if (!validate(props)) {
            throw new IllegalArgumentException();
        }

        if (null == System.getenv("HADOOP_HOME")) {
            throw new IllegalArgumentException("environment variable  HADOOP_HOME is null!!");
        }

        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", props.getProperty("hbase.zookeeper.property.clientPort"));
        configuration.set("hbase.zookeeper.quorum", props.getProperty("hbase.zookeeper.quorum"));
        configuration.set("hbase.master", props.getProperty("hbase.master"));

        connection = ConnectionFactory.createConnection(configuration);
    }

    /**
     * 验证参数
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
    public void saveDataPackObject(String rowKey, DataPackObject data) throws Exception {

        //保存二级索引
        Table indexTable = connection.getTable(TableName.valueOf(SECOND_INDEX_TABLE));
        String secondIndexRowKey = RowKeyUtil.makeDetectionTimeIndexRowKey(DataPackObjectUtils.convertDetectionDateToString(data.getDetectionTime()),
                data.getVin(), DataPackObjectUtils.getDataType(data));
        Put indexPut = new Put(secondIndexRowKey.getBytes());// 一个PUT代表一行数据，再NEW一个PUT表示第二行数据,每行一个唯一的ROWKEY，此处rowkey为put构造方法中传入的值
        indexPut.addColumn(COLUMN_FAMILY.getBytes("UTF-8"), COLUMN_DATA.getBytes("UTF-8"),
                rowKey.getBytes("UTF-8"));
        indexTable.put(indexPut);


        //保存数据
        Table dataTable = connection.getTable(TableName.valueOf(DataPackObjectUtils.getTableName(DataPackObjectUtils.getDataType(data))));//tabel对象线程不安全
        Put dataPut = new Put(rowKey.getBytes());// 一个PUT代表一行数据，再NEW一个PUT表示第二行数据,每行一个唯一的ROWKEY，此处rowkey为put构造方法中传入的值
        dataPut.addColumn(COLUMN_FAMILY.getBytes("UTF-8"), COLUMN_DATA.getBytes("UTF-8"),
                DataPackObjectUtils.toJson(data).getBytes("UTF-8"));
        dataTable.put(dataPut);
    }


    @Override
    public void saveVin(String vin) throws Exception {

        Table dataTable = connection.getTable(TableName.valueOf(VIN_TABLE));//tabel对象线程不安全
        Put dataPut = new Put(vin.getBytes());// 一个PUT代表一行数据，再NEW一个PUT表示第二行数据,每行一个唯一的ROWKEY，此处rowkey为put构造方法中传入的值
        dataPut.addColumn(COLUMN_FAMILY.getBytes("UTF-8"), COLUMN_DATA.getBytes("UTF-8"),
                vin.getBytes("UTF-8"));
        dataTable.put(dataPut);

    }

    @Override
    public void close() {
        if (null != connection) {
            try {
                connection.close();
            } catch (IOException e) {
                s_logger.error(e.getMessage());
            }
        }

    }
}
