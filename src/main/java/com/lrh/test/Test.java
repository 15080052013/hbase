package com.lrh.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class Test {

    private static Connection connection = null;
    private static Admin admin = null;


    static {
        try {
            Configuration configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    //ddl

    public static boolean isTableExist(String tableName) throws IOException {

        boolean exists = admin.tableExists(TableName.valueOf(tableName));
        return exists;
    }


    public static void createTable(String tableName,String... cfs) throws IOException {

        if (cfs.length<=0){
            System.out.println("请输入列族");
            return;
        }
        if (isTableExist(tableName)){
            System.out.println(tableName+"表已经存在");
            return;
        }
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        for (String cf : cfs) {
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
            hTableDescriptor.addFamily(hColumnDescriptor) ;
        }
        admin.createTable(hTableDescriptor);
    }

    public static void  close(){

        try {
            admin.close();
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
    public static void dropTable(String tableName) throws IOException {
        if (!isTableExist(tableName)){
            System.out.println(tableName+"表不存在");
            return;
        }
        admin.disableTable(TableName.valueOf(tableName));
        admin.deleteTable(TableName.valueOf(tableName));
    }


    //dml

    public static void addRowData(String tableName, String rowKey, String columnFamily, String

            column, String value) throws IOException{

//创建 HTable 对象
        Table table = connection.getTable(TableName.valueOf(tableName));
        System.out.println("connection = " + connection);
//向表中插入数据

        Put put = new Put(Bytes.toBytes(rowKey)); //向 Put 对象中组装数据

        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));

        table.put(put);

        table.close();

        System.out.println("插入数据成功");

    }

    public static void getData(String tableName, String rowKey, String columnFamily, String column) throws IOException {

        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));

        get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column));
        Result result = table.get(get);
        for (Cell cell : result.rawCells()) {
            System.out.println("CF:"+Bytes.toString(CellUtil.cloneFamily(cell))+
                    ",CN:"+Bytes.toString(CellUtil.cloneQualifier(cell))+
                    ",value:"+Bytes.toString(CellUtil.cloneValue(cell)));
        }

    }
    public static void main(String[] args) throws IOException {
        System.out.println(isTableExist("wsnd"));
//        createTable("wsnd","info1","info2");
//        dropTable("wsnd");
//        System.out.println(isTableExist("wsnd"));
        addRowData("wsnd","1002","info1","name","wqc");
        getData("wsnd","1002","info1","name");
        system.out.println("lrh");
        close();

    }
}
