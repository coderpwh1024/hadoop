package com.coderpwh.service.impl;

import com.coderpwh.service.HbaseService;
import jakarta.annotation.Resource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Service;

/**
 * @author coderpwh
 */
@Service
public class HbaseServiceImpl implements HbaseService {


    @Resource
    private Configuration configuration;


    @Resource
    private Connection connection;


    @Resource
    private Admin admin;


    /***
     * 创建数据库
     * @return
     */
    @Override
    public String createBase() {
        init();
        createTable("student",new String[]{"score"});
        insertData("student","zhangsan","score","English","69");
        insertData("student","zhangsan","score","Math","86");
        insertData("student","zhangsan","score","Computer","77");
        getData("student", "zhangsan", "score","English");
        close();
        return "success";
    }


    /***
     * 初始化
     */
    public void init() {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.rootdir", "hdfs://192.168.31.101:9000/hbase");
        try {
            configuration = HBaseConfiguration.create(configuration);
            admin = connection.getAdmin();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /***
     * 关闭连接
     */
    public void close() {
        try {
            if (admin != null) {
                admin.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (Exception e) {
            e.printStackTrace();

        }
    }


    /***
     * 创建表
     * @param myTableName
     * @param colFamily
     */
    public void createTable(String myTableName, String[] colFamily) {
        try {
            TableName tableName = TableName.valueOf(myTableName);

            if (admin.tableExists(tableName)) {
                System.out.println("表已经存在");
            } else {
                TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);
                for (String cf : colFamily) {
                    ColumnFamilyDescriptor family =
                            ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(cf)).build();
                    builder.setColumnFamily(family);
                }
                admin.createTable(builder.build());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /***
     * 添加数据
     * @param tableName
     * @param rowKey
     * @param colFamily
     * @param col
     * @param val
     */

    public void insertData(String tableName, String
            rowKey, String colFamily, String col, String val) {
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Put put = new Put(rowKey.getBytes());
            put.addColumn(colFamily.getBytes(), col.getBytes(), val.getBytes());
            table.put(put);
            table.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /***
     * 查询数据
     * @param tableName
     * @param rowKey
     * @param colFamily
     * @param col
     */
    public void getData(String tableName,String rowKey,String
            colFamily, String col){
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Get get = new Get(rowKey.getBytes());
            get.addColumn(colFamily.getBytes(),col.getBytes());
            Result result = table.get(get);
            System.out.println(new
                    String(result.getValue(colFamily.getBytes(),col==null?null:col.getBytes())));
            table.close();
        }catch (Exception e){
            e.printStackTrace();
        }

    }





}
