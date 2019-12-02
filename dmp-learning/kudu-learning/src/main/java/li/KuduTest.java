package li;

import li.bean.User;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.util.*;


public class KuduTest {
    KuduClient client = null;

    KuduClient.KuduClientBuilder clientBuilder = null;

    String tableName = "kuduFirstTable";

    //设置需要创建表的列名,这里需要指定主键，这里我指定的是id
    List<ColumnSchema> columnsSchema = new ArrayList<>();

    /**
     * @Author li
     * @Description 获得连接
     * @Date 21:31 2019/11/23
     * @Param []
     * @return void
     **/
    @Before
    public void getConn() {
        columnsSchema.add(new ColumnSchema.ColumnSchemaBuilder("id", Type.INT32).key(true).build());
        columnsSchema.add(new ColumnSchema.ColumnSchemaBuilder("name", Type.STRING).key(false).build());
        columnsSchema.add(new ColumnSchema.ColumnSchemaBuilder("gender", Type.STRING).key(false).build());
        columnsSchema.add(new ColumnSchema.ColumnSchemaBuilder("age", Type.INT32).key(false).build());

        List<String> masterAddresses = new ArrayList<>();
        masterAddresses.add("hadoop4:7051");
        masterAddresses.add("hadoop5:7051");
        masterAddresses.add("hadoop6:7051");
        clientBuilder = new KuduClient.KuduClientBuilder(masterAddresses);
        client = clientBuilder
                //设置超时时间，默认值是10s
                .defaultSocketReadTimeoutMs(6000)
                .build();
    }

    /**
     * @Author li
     * @Description 创建kudu表
     * @Date 21:31 2019/11/23
     * @Param []
     * @return void
     **/
    @Test
    public void create() throws KuduException {

        Schema schema = new Schema(columnsSchema);

        //这里设置根据那个列名分区
        List<String> columns = new ArrayList<>();
        columns.add("id");

        /**
         * public CreateTableOptions addHashPartitions(
         *  List<String> columns,
         *  int buckets)
         */
        CreateTableOptions tableOptions = new CreateTableOptions();
        tableOptions.addHashPartitions(columns, 3);
        tableOptions.setNumReplicas(3);

        /**
         * public KuduTable createTable(
         *  String name,
         *  Schema schema,
         *  CreateTableOptions builder)
         */
        KuduTable kuduTable = client.createTable(
                tableName,
                schema,
                tableOptions
        );

        System.out.println(kuduTable.getTableId());
    }

    /**
     * @Author li
     * @Description 更新和插入数据
     * @Date 21:32 2019/11/23
     * @Param []
     * @return void
     **/
    @Test
    public void upSert() throws KuduException {

        KuduTable kuduTable = client.openTable(tableName);

        KuduSession kuduSession = client.newSession();

        //设置手动刷新数据到表中
        kuduSession.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);

        //设置缓存大小
        kuduSession.setMutationBufferSpace(3000);

        Random random = new Random();
        for (int i = 0; i < 100; i++) {
            //这里的upsert必须在循环内，每次都是导入一行数据
            Upsert upsert = kuduTable.newUpsert();
            PartialRow row = upsert.getRow();
            int num = random.nextInt(100);
            row.addInt("id", i);
            row.addString("name", "张三-" + i);
            row.addString("gender", num % 2 == 0 ? "男" : "女");
            row.addInt("age",num);
            kuduSession.apply(upsert);
        }
        kuduSession.flush();

        kuduSession.close();
    }

    /**
     * @Author li
     * @Description 按照要求插询数据
     * @Date 21:32 2019/11/23
     * @Param []
     * @return void
     **/
    @Test
    public void query() throws KuduException, InvocationTargetException, IllegalAccessException {
        KuduTable kuduTable = client.openTable(tableName);
        KuduScanner.KuduScannerBuilder scannerBuilder = client.newScannerBuilder(kuduTable);

        //这里的true和false是根据创建表时候指定的主键
        ColumnSchema columnId = new ColumnSchema.ColumnSchemaBuilder("id",Type.INT32).key(true).build();
        ColumnSchema columnGender = new ColumnSchema.ColumnSchemaBuilder("gender",Type.STRING).key(false).build();

        /**
         * 增加插询的要求。性别为男，id小于等于20
         * public static KuduPredicate newComparisonPredicate(
         *  ColumnSchema column,
         *  ComparisonOp op,
         *  long value)
         */
        KuduPredicate predicateGender = KuduPredicate.newComparisonPredicate(
                columnGender,
                KuduPredicate.ComparisonOp.EQUAL,
                "男"
        );

        KuduPredicate predicateId = KuduPredicate.newComparisonPredicate(
                columnId,
                KuduPredicate.ComparisonOp.LESS_EQUAL,
                20
        );

        scannerBuilder.addPredicate(predicateGender);
        scannerBuilder.addPredicate(predicateId);

        KuduScanner scanner = scannerBuilder.build();

        while (scanner.hasMoreRows()){
            RowResultIterator rowResults = scanner.nextRows();
            while (rowResults.hasNext()){
                RowResult result = rowResults.next();
                HashMap<String, Object> map = new HashMap<>();
                map.put("id",result.getInt("id"));
                map.put("name",result.getString("name"));
                map.put("gender",result.getString("gender"));
                map.put("age",result.getInt("age"));
                User user = new User();
                BeanUtils.populate(user,map);
                System.out.println(user);
            }
        }
        scanner.close();
    }

    /**
     * @Author li
     * @Description 删除数据
     * @Date 21:32 2019/11/23
     * @Param []
     * @return void
     **/
    @Test
    public void delect() throws KuduException {
        KuduTable kuduTable = client.openTable(tableName);
        KuduSession kuduSession = client.newSession();
        Delete delete = kuduTable.newDelete();
        PartialRow row = delete.getRow();
        row.addInt("id",0);
        kuduSession.apply(delete);
        kuduSession.flush();
        kuduSession.close();
    }

    /**
     * @Author li
     * @Description 删除表
     * @Date 21:32 2019/11/23
     * @Param []
     * @return void
     **/
    @Test
    public void dropTable() throws KuduException {
        if (client.tableExists(tableName)){
            DeleteTableResponse response = client.deleteTable(tableName);
            System.out.println(response);
        }
    }


    @Test
    public void createKuduTableRange() throws KuduException {
        CreateTableOptions tableOptions = new CreateTableOptions();

        List<String> columns = new ArrayList<>();
        columns.add("id");
        //设置分区策略为范围分区
        /**
         *
         * public CreateTableOptions setRangePartitionColumns(List<String> columns)
         */
        tableOptions.setRangePartitionColumns(columns);

        Schema schema = new Schema(columnsSchema);

        /**
         * 设置分区范围
         * public PartialRow(Schema schema)
         */
        PartialRow upper = new PartialRow(schema);
        upper.addInt("id",50);
        PartialRow lower80 = new PartialRow(schema);
        lower80.addInt("id",80);
        tableOptions.addRangePartition(new PartialRow(schema),upper);
        tableOptions.addRangePartition(upper,lower80);
        tableOptions.addRangePartition(lower80,new PartialRow(schema));

        //设置三个备份
        tableOptions.setNumReplicas(3);

        KuduTable table = client.createTable("rangePartition", schema, tableOptions);

        System.out.println(table.getTableId());
    }

    /**
     * @Author li
     * @Description 关闭资源
     * @Date 21:32 2019/11/23
     * @Param []
     * @return void
     **/
    @After
    public void close() {
        try {
            client.close();
        } catch (KuduException e) {
            e.printStackTrace();
        }
    }
}
