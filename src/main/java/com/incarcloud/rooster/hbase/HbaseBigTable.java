package com.incarcloud.rooster.hbase;/**
 * Created by fanbeibei on 2017/7/10.
 */

import com.incarcloud.rooster.bigtable.IBigTable;
import com.incarcloud.rooster.datapack.DataPackObject;
import com.incarcloud.rooster.util.DataPackObjectUtils;
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
            throw new IllegalArgumentException("hadoop home is null!!");
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
    public void save(String rowKey, DataPackObject data, String tableName) throws Exception {
        Table table = connection.getTable(TableName.valueOf(tableName));//tabel对象线程不安全

        Put put = new Put(rowKey.getBytes());// 一个PUT代表一行数据，再NEW一个PUT表示第二行数据,每行一个唯一的ROWKEY，此处rowkey为put构造方法中传入的值
        put.addColumn(COLUMN_FAMILY.getBytes("UTF-8"), COLUMN_DATA.getBytes("UTF-8"),
                DataPackObjectUtils.toJson(data).getBytes("UTF-8"));
        table.put(put);
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
