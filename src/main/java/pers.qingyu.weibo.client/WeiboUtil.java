package pers.qingyu.weibo.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

public class WeiboUtil {

	/**
	 * 获取配置文件
	 */
	//获取hbase配置信息
	public static Configuration getConfiguration() {
		return HBaseConfiguration.create();
	}


	/**
	 * 创建命名空间
	 */
	public static void createNamespace(String spaceName) throws IOException {


		//获取hbase管理员对象
		Connection connection = ConnectionFactory.createConnection(getConfiguration());
		Admin admin = connection.getAdmin();

		//获取命名空间描述器
		NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(spaceName).build();

		//创建namespace
		admin.createNamespace(namespaceDescriptor);

		admin.close();
		connection.close();
	}

	/**
	 * 创建表
	 */
	public static void createTable(String tableName, int versions, String... cfs) throws IOException {

		//获取hbase管理员对象
		Connection connection = ConnectionFactory.createConnection(getConfiguration());
		Admin admin = connection.getAdmin();

		//创建表描述器
		HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));

		for (String cf : cfs) {
			HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
			hColumnDescriptor.setMaxVersions(versions);
			hTableDescriptor.addFamily(hColumnDescriptor);
		}

		admin.createTable(hTableDescriptor);

		admin.close();
		connection.close();
	}

	public static void postContent(String tableName, String uid, String cf, String cn, String value) throws IOException {

		Connection connection = ConnectionFactory.createConnection(getConfiguration());
		Table table = connection.getTable(TableName.valueOf(tableName));

		//封装put
		long ts = System.currentTimeMillis();
		String rowKey = uid + "_" + ts;
		Put put = new Put(Bytes.toBytes(rowKey));
		put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn), ts, Bytes.toBytes(value));

		//执行操作
		table.put(put);

		Table inboxTable = connection.getTable(TableName.valueOf(Weibo.INBOX_TABLE));
		Table relationTable = connection.getTable(TableName.valueOf(Weibo.RELATION_TABLE));

		//实时更新
		Get get = new Get(Bytes.toBytes(uid));
		Result result = relationTable.get(get);

		ArrayList<Put> puts = new ArrayList<>();

		for (Cell cell : result.rawCells()) {
			if ("fans".equals(CellUtil.cloneFamily(cell))) {
				byte[] inboxRowKey = CellUtil.cloneQualifier(cell);

				Put inboxPut = new Put(inboxRowKey);
				inboxPut.addColumn(Bytes.toBytes("info"), Bytes.toBytes(uid), ts, Bytes.toBytes(rowKey));

				puts.add(inboxPut);
			}
		}

		inboxTable.put(puts);

		table.close();
		inboxTable.close();
		relationTable.close();
		connection.close();

	}

	/**
	 * 添加关注用户(多个)
	 * 1.在用户关系表中，给当前用户添加attends
	 * 2.在用户关系表中，给被添加用户添加fans
	 * 3.在收件箱表中，给当前用户添加关注用户最近所发微博的rowKey
	 */
	public static void addAttends(String uid, String... attends) throws IOException {

		//1.在用户关系表中，给当前用户添加attends
		Connection connection = ConnectionFactory.createConnection(getConfiguration());
		Table table = connection.getTable(TableName.valueOf(Weibo.RELATION_TABLE));

		Put attendPut = new Put(Bytes.toBytes(uid));

		//存在被添加用户的添加对象
		ArrayList<Put> puts = new ArrayList<>();

		for (String attend : attends) {
			attendPut.addColumn(Bytes.toBytes("attends"), Bytes.toBytes(attend), Bytes.toBytes(""));

			//2.在用户关系表中，给被添加用户添加fans
			Put put = new Put(Bytes.toBytes(attend));
			put.addColumn(Bytes.toBytes("fans"), Bytes.toBytes(uid), Bytes.toBytes(""));
			puts.add(put);
		}

		puts.add(attendPut);
		table.put(puts);

		// 3.在收件箱表中，给当前用户添加关注用户最近所发微博的rowKey
		Table inboxTable = connection.getTable(TableName.valueOf(Weibo.INBOX_TABLE));
		Table connectionTable = connection.getTable(TableName.valueOf(Weibo.CONTENT_TABLE));

		Put inboxPut = new Put(Bytes.toBytes(uid));

		for (String attend : attends) {
//			StartRow - StopRow 方式获取
//			Scan scan = new Scan(Bytes.toBytes(attend), Bytes.toBytes(attend + "|"));

			Scan scan = new Scan();

			scan.setFilter(new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(attend + "_")));

			ResultScanner scanner = connectionTable.getScanner(scan);

			for (Result result : scanner) {
				byte[] row = result.getRow();
				inboxPut.addColumn(Bytes.toBytes("info"), Bytes.toBytes("attend"), row);

				//往收件表中给操作者添加数据
				inboxTable.put(inboxPut);
			}
		}


		inboxTable.close();
		table.close();
		connection.close();
	}

	/**
	 * 取消关注
	 * 1.在用户关系表中，删除当前用户的attends
	 * 2.在用户关系表中，删除被取关用户的fans（操作者）
	 * 3.在收件箱表中删除取关用户的所有数据
	 */
	public static void deleteRelation(String uid, String... deletes) throws IOException {

		Connection connection = ConnectionFactory.createConnection(getConfiguration());
		Table relationTable = connection.getTable(TableName.valueOf(Weibo.RELATION_TABLE));

		ArrayList<Delete> deletesList = new ArrayList<>();

		//1.在用户关系表中，删除当前用户的attends
		Delete userDelete = new Delete(Bytes.toBytes(uid));

		for (String delete : deletes) {
			userDelete.addColumn(Bytes.toBytes("attends"), Bytes.toBytes(delete));

			//2.在用户关系表中，删除被取关用户的fans（操作者）
			Delete fansDelete = new Delete(Bytes.toBytes(delete));
			fansDelete.addColumn(Bytes.toBytes("fans"), Bytes.toBytes(uid));
			deletesList.add(fansDelete);
		}

		deletesList.add(userDelete);
		relationTable.delete(deletesList);

		//3.在收件箱表中删除取关用户的所有数据
		Table inboxTable = connection.getTable(TableName.valueOf(Weibo.INBOX_TABLE));

		Delete inboxDelete = new Delete(Bytes.toBytes(uid));

		for (String delete : deletes) {
			inboxDelete.addColumn(Bytes.toBytes("info"), Bytes.toBytes(delete));
		}
		inboxTable.delete(inboxDelete);

		relationTable.close();
		inboxTable.close();
		connection.close();
	}

	/**
	 * 获取关注人的微博内容
	 */
	public static void getWeibo(String uid) throws IOException {

		Connection connection = ConnectionFactory.createConnection(getConfiguration());
		Table inboxTable = connection.getTable(TableName.valueOf(Weibo.INBOX_TABLE));
		Table contentTable = connection.getTable(TableName.valueOf(Weibo.CONTENT_TABLE));

		Get get = new Get(Bytes.toBytes(uid));
		get.setMaxVersions(3);

		Result result = inboxTable.get(get);

		for (Cell cell : result.rawCells()) {
			byte[] contentRowKey = CellUtil.cloneValue(cell);

			Get contentGet = new Get(contentRowKey);
			Result contentResult = contentTable.get(contentGet);

			for (Cell rawCell : contentResult.rawCells()) {
				String uid_ts = Bytes.toString(CellUtil.cloneRow(rawCell));
				String id = uid_ts.split("_")[0];
				String ts = uid_ts.split("_")[1];

				String date = new SimpleDateFormat("yyyy-dd-MM HH:mm:ss").format(new Date(Long.parseLong(ts)));
				System.out.println("用户：" + id + ", Time:" + date + ", content:" + Bytes.toString(CellUtil.cloneValue(rawCell)));
			}
		}

		inboxTable.close();
		contentTable.close();
		connection.close();
	}

}
