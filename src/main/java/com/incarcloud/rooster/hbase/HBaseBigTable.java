package com.incarcloud.rooster.hbase;

import com.incarcloud.rooster.bigtable.IBigTable;
import com.incarcloud.rooster.datapack.DataPackObject;
import com.incarcloud.rooster.util.DataPackObjectUtil;
import com.incarcloud.rooster.util.RowKeyUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

/**
 * @author Fan Beibei
 * @Description: HBase 操作类，单例使用
 * @date 2017/7/10 18:04
 */
public class HBaseBigTable implements IBigTable {

    /**
     * Logger
     */
    private static Logger logger = LoggerFactory.getLogger(HBaseBigTable.class);

    /**
     * 车辆VIN存储表名
     */
    private static String TABLE_NAME_VEHICLE;

    /**
     * 车辆数据存储表名
     */
    private static String TABLE_NAME_TELEMETRY;

    /**
     * 列族名称
     */
    private static final String COLUMN_FAMILY_NAME = "base";

    /**
     * 数据列名称
     */
    private static final String COLUMN_NAME_DATA = "data";

    /**
     * 隐藏列名称
     */
    private static final String COLUMN_NAME_HIDDEN = "hidden";

    /**
     * HBase连接，重量级且线程安全，建议单例
     */
    private Connection connection;

    /**
     * 默认构造函数，手动创建指定数据表
     *
     * @param props              HBase连接属性
     * @param telemetryTableName 车辆数据存储表名
     * @param vehicleTableName   车辆VIN存储表名
     * @throws IOException
     */
    public HBaseBigTable(Properties props, String telemetryTableName, String vehicleTableName) throws IOException {
        // 校验参数合法性
        if (!validate(props)) {
            throw new IllegalArgumentException();
        }
        if (null == System.getenv("HADOOP_HOME")) {
            throw new IllegalArgumentException("environment variable 'HADOOP_HOME' is null!");
        }
        if (StringUtils.isBlank(vehicleTableName) || StringUtils.isBlank(telemetryTableName)) {
            throw new IllegalArgumentException("storage table name is null!");
        }

        // 创建HBase连接对象
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", props.getProperty("hbase.zookeeper.property.clientPort"));
        configuration.set("hbase.zookeeper.quorum", props.getProperty("hbase.zookeeper.quorum"));
        configuration.set("hbase.master", props.getProperty("hbase.master"));
        connection = ConnectionFactory.createConnection(configuration);

        // 设置数据表常量
        TABLE_NAME_TELEMETRY = telemetryTableName;
        TABLE_NAME_VEHICLE = vehicleTableName;
    }

    /**
     * 验证参数
     *
     * @param props 连接属性
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

    /**
     * 获得HBase连接对象
     *
     * @return
     */
    public Connection getConnection() {
        return this.connection;
    }

    @Override
    public void saveDataPackObject(String rowKey, DataPackObject data) throws Exception {
        // Table对象线程不安全
        Table table = connection.getTable(TableName.valueOf(TABLE_NAME_TELEMETRY));

        // 一个PUT代表一行数据，rowKey由参数列表传入
        Put put = new Put(rowKey.getBytes());
        put.addColumn(Bytes.toBytes(COLUMN_FAMILY_NAME), Bytes.toBytes(COLUMN_NAME_DATA), Bytes.toBytes(DataPackObjectUtil.toJson(data)));
        table.put(put);

        // 打印日志
        logger.debug("Save data pack object for vin({}) by {} success!", data.getVin(), rowKey);
    }

    @Override
    public void saveVin(String vin) throws Exception {
        // Table对象线程不安全
        Table dataTable = connection.getTable(TableName.valueOf(TABLE_NAME_VEHICLE));

        // 一个PUT代表一行数据，rowKey为车辆VIN
        Put put = new Put(vin.getBytes());
        put.addColumn(Bytes.toBytes(COLUMN_FAMILY_NAME), Bytes.toBytes(COLUMN_NAME_DATA), Bytes.toBytes(vin));
        dataTable.put(put);

        // 打印日志
        logger.debug("Save vin({}) success!", vin);
    }

    @Override
    public <T extends DataPackObject> T getData(String rowKey, Class<T> clazz) {
        // 验证参数信息
        if (StringUtils.isBlank(rowKey)) {
            throw new IllegalArgumentException("the row key can't be null");
        }

        try {
            // 读取数据
            Table table = connection.getTable(TableName.valueOf(TABLE_NAME_TELEMETRY));
            Result result = table.get(new Get(Bytes.toBytes(rowKey)));

            // 获得json字符串
            String jsonString = Bytes.toString(result.getValue(Bytes.toBytes(COLUMN_FAMILY_NAME), Bytes.toBytes(COLUMN_NAME_DATA)));

            // 使用属性名id装载RowKey值
            T data = DataPackObjectUtil.fromJson(jsonString, clazz);
            data.setId(Bytes.toString(result.getRow()));

            // 返回结果
            return data;

        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    @Override
    public <T extends DataPackObject> T getData(String vin, Class<T> clazz, Sort sort) {
        // 读取最早或最近的一条记录
        List<T> dataList = queryData(vin, clazz, sort, null, null, 1, null);
        if (null != dataList && 1 == dataList.size()) {
            // 返回数据记录
            return dataList.get(0);
        }
        return null;
    }

    @Override
    public <T extends DataPackObject> List<T> queryData(String vin, Class<T> clazz, Date startTime, Date endTime) {
        // 验证参数信息
        if (null == vin || null == startTime || null == endTime) {
            throw new IllegalArgumentException("the params can't be null");
        }
        // 查询开始时间必须小于结束时间
        if (startTime.getTime() > endTime.getTime()) {
            throw new IllegalArgumentException("the end time must be bigger than the start time");
        }

        // 读取数据
        try {
            // 根据开始和结束时间查询数据
            Table table = connection.getTable(TableName.valueOf(TABLE_NAME_TELEMETRY));

            // 构建查询条件
            Scan scan = new Scan();
            // 计算查询区间
            String startTimeRowKey = RowKeyUtil.makeMinRowKey(vin, DataPackObjectUtil.getDataType(clazz), DataPackObjectUtil.convertDetectionTimeToString(startTime));
            String stopTimeRowKey = RowKeyUtil.makeMinRowKey(vin, DataPackObjectUtil.getDataType(clazz), DataPackObjectUtil.convertDetectionTimeToString(endTime));
            // 设置查询数据范围
            scan.setStartRow(Bytes.toBytes(startTimeRowKey));
            scan.setStopRow(Bytes.toBytes(stopTimeRowKey));

            // 遍历查询结果集
            String jsonString;
            T data;
            List<T> dataList = new ArrayList<>();
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                //System.out.println(Bytes.toString(dataResult.getRow()));
                // 获得json字符串
                jsonString = Bytes.toString(result.getValue(Bytes.toBytes(COLUMN_FAMILY_NAME), Bytes.toBytes(COLUMN_NAME_DATA)));
                if (StringUtils.isNotBlank(jsonString)) {
                    try {
                        //System.out.println(Bytes.toString(result.getRow()));
                        // 转换为json对象
                        data = DataPackObjectUtil.fromJson(jsonString, clazz);
                        // 使用属性名id装载RowKey值
                        data.setId(Bytes.toString(result.getRow()));
                        // 添加返回值
                        dataList.add(data);
                    } catch (Exception e) {
                        logger.error("queryData: json转object异常, ", e);
                    }
                }
            }

            // 释放资源
            scanner.close();

            // 返回数据集
            return dataList;

        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public <T extends DataPackObject> List<T> queryData(String vin, Class<T> clazz, Integer pageSize, String startKey) {
        return queryData(vin, clazz, Sort.DESC, null, null, pageSize, startKey);
    }

    @Override
    public <T extends DataPackObject> List<T> queryData(String vin, Class<T> clazz, Sort sort, Date startTime, Date endTime, Integer pageSize, String startKey) {
        // 验证参数信息
        if (null == vin || null == pageSize) {
            throw new IllegalArgumentException("the params can't be null");
        }

        // 读取数据
        try {
            // 查询表
            Table table = connection.getTable(TableName.valueOf(TABLE_NAME_TELEMETRY));

            // 构建查询条件
            Scan scan = new Scan();

            // 计算start和stop键值
            String startRowKey = startKey;
            String stopRowKey;
            if (null == sort || Sort.DESC == sort) {
                // 如果不传startKey，按照时间倒序查询
                if (StringUtils.isBlank(startKey)) {
                    // 判断是否设置了查询结束时间
                    if (null == endTime) {
                        // 查询范围比较大
                        startRowKey = RowKeyUtil.makeMaxRowKey(vin, clazz);
                    } else {
                        // 查询范围比较小
                        startRowKey = RowKeyUtil.makeMaxRowKey(vin, clazz, endTime);
                    }
                }

                // 判断是否设置了查询开始时间
                if (null == startTime) {
                    // 查询范围比较大
                    stopRowKey = RowKeyUtil.makeMinRowKey(vin, clazz);
                } else {
                    // 查询范围比较小
                    stopRowKey = RowKeyUtil.makeMinRowKey(vin, clazz, startTime);
                }

                // 按照时间倒序
                scan.setReversed(true);
            } else {
                // 如果不传startKey，按照时间升序查询
                if (StringUtils.isBlank(startKey)) {
                    // 判断是否设置了查询开始时间
                    if (null == startTime) {
                        // 查询范围比较大
                        startRowKey = RowKeyUtil.makeMinRowKey(vin, clazz);
                    } else {
                        // 查询范围比较小
                        startRowKey = RowKeyUtil.makeMinRowKey(vin, clazz, startTime);
                    }
                }

                // 判断是否设置了查询结束时间
                if (null == endTime) {
                    // 查询范围比较大
                    stopRowKey = RowKeyUtil.makeMaxRowKey(vin, clazz);
                } else {
                    // 查询范围比较小
                    stopRowKey = RowKeyUtil.makeMaxRowKey(vin, clazz, startTime);
                }
            }

            // String转Bytes
            byte[] startRowBytes = Bytes.toBytes(startRowKey);
            startRowBytes = Bytes.copy(startRowBytes, 0, startRowBytes.length - 1); //包含关系
            byte[] stopRowBytes = Bytes.toBytes(stopRowKey);

            // 设置查询数据范围
            scan.setStartRow(startRowBytes);
            scan.setStopRow(stopRowBytes);

            // 构建过滤器
            FilterList filterList = new FilterList();
            filterList.addFilter(new SkipFilter(new SingleColumnValueFilter(Bytes.toBytes(COLUMN_FAMILY_NAME),
                    Bytes.toBytes(COLUMN_NAME_HIDDEN),
                    CompareFilter.CompareOp.EQUAL,
                    new NullComparator()))); //单列值过滤器
            filterList.addFilter(new PageFilter(pageSize)); //分页过滤器

            // 设置过滤器
            scan.setFilter(filterList);

            // 遍历查询结果集
            String jsonString;
            T data;
            List<T> dataList = new ArrayList<>();
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                // 获得json字符串
                jsonString = Bytes.toString(result.getValue(Bytes.toBytes(COLUMN_FAMILY_NAME), Bytes.toBytes(COLUMN_NAME_DATA)));
                if (StringUtils.isNotBlank(jsonString)) {
                    try {
                        //System.out.println(Bytes.toString(result.getRow()));
                        // 转换为json对象
                        data = DataPackObjectUtil.fromJson(jsonString, clazz);
                        // 使用属性名id装载RowKey值
                        data.setId(Bytes.toString(result.getRow()));
                        // 添加返回值
                        dataList.add(data);
                    } catch (Exception e) {
                        logger.error("queryData: json转object异常, ", e);
                    }
                }
            }

            // 释放资源
            scanner.close();

            // 返回数据集
            return dataList;

        } catch (IOException e) {
            e.printStackTrace();
        }

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
