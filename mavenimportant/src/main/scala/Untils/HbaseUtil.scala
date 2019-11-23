package Untils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}

/**
 * @Author li
 * @Description hbase中的对表的增删查
 * @Date 14:55 2019/11/3
 **/
object HbaseUtil {

  private val conf: Configuration = HBaseConfiguration.create()

  //创建hbase连接
  private val connection: Connection = ConnectionFactory.createConnection(conf)

  //获取操作集群实例对象，也就是客户端对象
  private val admin: Admin = connection.getAdmin

  /**
    * @Author li
    * @Description 这里会出现线程不安全问题。但是项目中创建表都是直接在hbase-shell中直接创建的
    * @Date 11:30 2019/11/3
    * @Param [tableName, family]
    * @return org.apache.hadoop.hbase.client.Table
    **/
  def initTable(tableName: String, family: String) = {

    //将string类型的装换为TableName
    val hbaesTableName: TableName = TableName.valueOf(tableName)

    /**
      * 如果表不存在就创建
      */
    if (!admin.tableExists(hbaesTableName)) {
      /** 新建表描述器
        * public HTableDescriptor(final TableName name)
        */
      val channelTableDescriptor = new HTableDescriptor(tableName)

      /**
        * 新建列簇描述器
        */
      val columnDescriptor = new HColumnDescriptor(family)

      /**
        * public void addFamily(final HColumnDescriptor family)
        */
      channelTableDescriptor.addFamily(columnDescriptor)

      /** 新建表
        * void createTable(HTableDescriptor desc)
        */
      admin.createTable(channelTableDescriptor)
    }

    /**
      * 如果存在就直接获取
      */
    connection.getTable(hbaesTableName)
  }

  /**
    * @Author li
    * @Description 通过rowkey获得一列数据
    * @Date 11:48 2019/11/3
    * @Param [tableName, family, rowkey, columnName]
    * @return java.lang.String
    **/
  def queryByRowkey(tableName: String, family: String, rowkey: String, columnName: String) = {
    val table: Table = initTable(tableName, family)

    var str: String = ""

    try {
      //因为rowkey在hbase中是以字节形式存储的
      val get = new Get(rowkey.getBytes())

      //更具rowkey返回表中所有的字段
      val result: Result = table.get(get)

      //根据列名获取相应的列值
      val bytes: Array[Byte] = result.getValue(family.getBytes(), columnName.getBytes())

      //判定取得的列值是否为空
      if (null != bytes && bytes.length > 0) str = new String(bytes)
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      table.close()
    }
    str
  }

  /**
    * @Author li
    * @Description 更具rowkey插入单列数据
    * @Date 11:56 2019/11/3
    * @Param [tableName, family, rowkey, columnName, columnValue]
    * @return void
    **/
  def putDataByRowkey(tableName: String, family: String, rowkey: String, columnName: String, columnValue: String) = {
    val table: Table = initTable(tableName, family)
    try {
      val put = new Put(rowkey.getBytes())

      /**
        * 构建需要插入的put
        */
      put.addColumn(family.getBytes(), columnName.getBytes(), columnValue.getBytes())

      /** 将数据插入到表中
        * void put(Put put)
        */
      table.put(put)
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      table.close()
    }
  }

  /**
   * @Author li
   * @Description 通过rowkey插入多列数据
   * @Date 14:46 2019/11/3
   * @Param [tableName, family, rowkey, map]
   * @return void
   **/
  def putListDataByRowkey(tableName: String, family: String, rowkey: String, map: Map[String, Any]) = {
    val table: Table = initTable(tableName, family)
    try {
      val put: Put = new Put(rowkey.getBytes())
      for (elem <- map) {
        put.addColumn(family.getBytes(), elem._1.toString.getBytes(), elem._2.toString.getBytes())
      }
      table.put(put)
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      table.close()
    }
  }

  /**
   * @Author li
   * @Description 根据rowkey将一列的数据删除
   * @Date 14:51 2019/11/3
   * @Param [tableName, family, rowkey, columnName]
   * @return void
   **/
  def delByRowkey(tableName: String, family: String, rowkey: String, columnName: String)={
    val table: Table = initTable(tableName,family)
    try{
      val delete = new Delete(rowkey.getBytes())
      //删除指定列的最新版本
      delete.addColumn(family.getBytes(),columnName.getBytes())
      table.delete(delete)
    }catch {
      case e:Exception => e.printStackTrace()
    }finally {
      table.close()
    }
  }
}
