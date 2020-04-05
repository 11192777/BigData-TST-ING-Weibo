package pers.qingyu.weibo.client;

import java.io.IOException;

public class Weibo {

	public static final String NAME_SPACE = "weibo";
	public static final String RELATION_TABLE = NAME_SPACE + ":relation";
	public static final String CONTENT_TABLE = NAME_SPACE + ":content";
	public static final String INBOX_TABLE = NAME_SPACE + ":inbox";


	public static void init() throws IOException {
		WeiboUtil.createNamespace(NAME_SPACE);

		//创建用户关系表
		WeiboUtil.createTable(RELATION_TABLE, 1, "attends", "fans");

		//创建微博内容表
		WeiboUtil.createTable(CONTENT_TABLE, 1, "info");

		//创建收件箱表
		WeiboUtil.createTable(INBOX_TABLE, 100, "info");

	}

	public static void main(String[] args) throws IOException {
//		init();

		//关注
		WeiboUtil.addAttends("1001", "1002","1003");

		//被关注的人发微博（多个人发微博）
//		WeiboUtil.postContent(Weibo.CONTENT_TABLE, "1002", "info", "content", "今天天气真晴朗！");
//		WeiboUtil.postContent(Weibo.CONTENT_TABLE, "1002", "info", "content", "我可去你妹夫的！");
//		WeiboUtil.postContent(Weibo.CONTENT_TABLE, "1003", "info", "content", "脑壳疼！");
//		WeiboUtil.postContent(Weibo.CONTENT_TABLE, "1001", "info", "content", "今天又熬夜！");

		//获取关注人的微博
//		WeiboUtil.getWeibo("1001");

		//关注已经发微博的人
//		WeiboUtil.addAttends("1002", "1001");

		//获取关注人发的微博
//		WeiboUtil.getWeibo("1002");

		//取消关注
		WeiboUtil.deleteRelation("1001", "1002");
		WeiboUtil.getWeibo("1001");

		//获取关注人的微博


	}
}
