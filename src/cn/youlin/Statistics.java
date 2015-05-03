package cn.youlin;

import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;

public class Statistics {

	public static void main(String[] args) throws ParseException, IOException {
		Statisticsx("2015-05-02");
		
		//jar包参数
//		if(args!=null && args.length==1)
//		{
//			Statisticsx(args[0]);
//		}
//		else{
//			Statisticsx(new SimpleDateFormat("yyyy-MM-dd").format(new Date()));
//		}
		
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void Statisticsx(String date) throws ParseException, IOException
	{
		System.out.println(date+" 统计结果------------------------------------------------");
		String[] datearray=date.split("-");
		String tname=datearray[0]+"_"+datearray[1]+"_"+datearray[2]+"_logs";
		
		//计算开始和结束时间的毫秒数
		SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		long starttime=sdf.parse(date+" 00:00:00").getTime();
		long endtime=sdf.parse(date+" 23:59:59").getTime();
		
		DBCollection dbc=null;
		
		List<Map> wblist=new ArrayList<Map>();
		
		String dburl = "jdbc:mysql://192.168.1.24:3306/youlinlog?useUnicode=true&amp;characterEncoding=UTF-8&allowMultiQueries=true&useOldAliasMetadataBehavior=true"; // MySQL配置时的用户名
		String user = "youlinweb_w"; // Java连接MySQL配置时的密码 String
		String password = "web@youlin_WR";
		PreparedStatement pstmt = null;
		Connection conn = null;
		ResultSet rs=null;

		try { // 加载驱动程序 Class.forName(driver); // 连续数据库
			conn = DriverManager.getConnection(dburl, user, password);
			if (!conn.isClosed()) {
//				System.out.println("Succeeded connecting to the Database!");
			}
			
			//PV
			pstmt=conn.prepareStatement("select count(*) as count from "+tname);
			rs=pstmt.executeQuery();
			while(rs.next()){
				String s=rs.getString("count");
				System.out.println(String.format("%-10s","PV:")+s);
				Map hm=new HashMap<String, String>();
				hm.put("k", "PV");
				hm.put("v", s);
				wblist.add(hm);
			}
			
			//累计用户数
			dbc=getMongoCollection("UserProfile");
			long countx=dbc.count(new BasicDBObject("accountStatus","setup_userProfile").append("registerDate", new BasicDBObject("$lt",new Date(endtime))));
			System.out.println(String.format("%-10s","累计用户数为: ")+countx);
			Map hm=new HashMap<String, String>();
			hm.put("k", "累计用户数为");
			hm.put("v", countx);
			wblist.add(hm);
			
			//UV(日活跃用户数)(日活跃率)
			pstmt=conn.prepareStatement("select count(*) as count from (select distinct (uid) as uid from "+tname+") as a");
			rs=pstmt.executeQuery();
			while(rs.next()){
				String s=rs.getString("count");
				System.out.println(String.format("%-10s","日活跃用户数: ")+s);
				System.out.println(String.format("%-10s","用户日活跃率: ")+Double.parseDouble(s)/countx);
				 hm=new HashMap<String, String>();
				hm.put("k", "日活跃用户数");
				hm.put("v", s);
				wblist.add(hm);
				
				 hm=new HashMap<String, String>();
				hm.put("k", "用户日活跃率");
				hm.put("v", Double.parseDouble(s)/countx);
				wblist.add(hm);
			}
			
			//(周UV/累计用户数)用户周活跃率
			SimpleDateFormat sdf1 =new SimpleDateFormat("yyyy-MM-dd");
			SimpleDateFormat sdf2 =new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			SimpleDateFormat sdf3 =new SimpleDateFormat("yyyy_MM_dd");
			
			String dstr=sdf1.format(new Date());
			long t2 = sdf2.parse(dstr+" 00:00:00").getTime();
//			long t1 = t2-7*24*3600*1000;
			long t1 = t2-5*24*3600*1000;
			
			List<String> tnlist=new ArrayList<String>();
			for(long i=t1;i<t2;i+=(1000*24*3600)){
				tnlist.add(sdf3.format(new Date(i))+"_logs");
			}
			
			StringBuffer sb=new StringBuffer();
			sb.append("select count(*) as count from (select distinct (uid) from (select distinct (uid) as uid from "+tnlist.get(0)+" ");
			tnlist.remove(0);
			for(String s:tnlist){
				sb.append("union all select distinct (uid) as uid from "+s+" ");
			}
			sb.append(")as a) as b");
			pstmt=conn.prepareStatement(sb.toString());
			rs=pstmt.executeQuery();
			while(rs.next()){
				String s=rs.getString("count");
				System.out.println(s);
				System.out.println(String.format("%-10s","用户最近五天活跃率:")+Double.parseDouble(s)/countx);
				hm=new HashMap<String, String>();
				hm.put("k", "用户最近五天活跃率");
				hm.put("v", Double.parseDouble(s)/countx);
				wblist.add(hm);
			}
			
			//每日ios与android比例(iOS、Android日活用户比例)
			double ios=0;
			double android=0;
			pstmt=conn.prepareStatement("select count(distinct uid) as c from "+tname+" where app='123456' or app='111111'");
			rs=pstmt.executeQuery();
			while(rs.next()){
				ios=rs.getInt("c");
			}
			pstmt=conn.prepareStatement("select count(distinct uid) as c from "+tname+" where app='654321' or app='999999'");
			rs=pstmt.executeQuery();
			while(rs.next()){
				android=rs.getInt("c");
			}
			System.out.println(String.format("%-10s","ios: ")+ios);
			System.out.println(String.format("%-10s","android: ")+android);
			hm=new HashMap<String, String>();
			hm.put("k", "ios/android");
			hm.put("v", ios+"个/"+android+"个");
			wblist.add(hm);
			
			//app版本比例(App版本日活用户比例)
			String v=null;
			String c=null;
			pstmt=conn.prepareStatement("select version as v,count(version) as c from (select version from youlinlog."+tname+" group by uid) as a group by version order by count(version) desc;");
			rs=pstmt.executeQuery();
			hm=new HashMap<String, String>();
			hm.put("k", "app版本使用情况如下：");
			hm.put("v", "");
			wblist.add(hm);
			while(rs.next()){
				v=rs.getString("v");
				c=rs.getString("c");
				System.out.println(String.format("%-10s",v+": ")+c);
				
				hm=new HashMap<String, String>();
				hm.put("k", v);
				hm.put("v", c);
				wblist.add(hm);
			}
			hm=new HashMap<String, String>();
			hm.put("k", " ");
			hm.put("v", "");
			wblist.add(hm);
			
			//首页小匠动态的点击人数
			pstmt=conn.prepareStatement("select count(*) as count from (select distinct (uid) from "+tname+" where url='/studio/myFollowNews') as a;");
			rs=pstmt.executeQuery();
			while(rs.next()){
				String s=rs.getString("count");
				System.out.println(String.format("%-10s","首页小匠动态的点击人数:")+s);
				hm=new HashMap<String, String>();
				hm.put("k", "首页小匠动态的点击人数");
				hm.put("v", s);
				wblist.add(hm);
			}
			
			//创建工作室的点击人数
			pstmt=conn.prepareStatement("select count(*) as count from (select distinct (uid) from "+tname+" where url='/studio/verifyStudioName') as a;");
			rs=pstmt.executeQuery();
			while(rs.next()){
				String s=rs.getString("count");
				System.out.println(String.format("%-10s","创建工作室的点击人数:")+s);
				hm=new HashMap<String, String>();
				hm.put("k", "创建工作室的点击人数");
				hm.put("v", s);
				wblist.add(hm);
			}
			
			//成功创建工作室人数，即小匠数
			dbc=getMongoCollection("studio");
			long countxj=dbc.count(new BasicDBObject("createTime",new BasicDBObject("$gt",starttime).append("$lt", endtime)));
			System.out.println(String.format("%-30s","成功创建工作室人数，即小匠数:_test")+countxj);
			hm=new HashMap<String, String>();
			hm.put("k", "成功创建工作室人数，即小匠数:_test");
			hm.put("v", countxj);
			wblist.add(hm);

			//小匠日活跃人数
			long countxjx=0;
			dbc=getMongoCollection("studio");
			pstmt=conn.prepareStatement("select distinct (uid) from "+tname+" where studioid is not null and studioid !='null'");
			rs=pstmt.executeQuery();
			while(rs.next()){
				DBCursor cursor = dbc.find(new BasicDBObject("userId",rs.getString("uid")));
				if(cursor.hasNext()){
					countxjx++;
				}
			}
			System.out.println(String.format("%-10s","小匠日活跃人数:_test")+countxjx);
			hm=new HashMap<String, String>();
			hm.put("k", "小匠日活跃人数:_test");
			hm.put("v", countxjx);
			wblist.add(hm);
			
			//注册成功人数
			pstmt=conn.prepareStatement("select count(*) as count from (select distinct (uid) from "+tname+" where url='/userProfile/modifyUserProfile/v2') as a;");
			rs=pstmt.executeQuery();
			while(rs.next()){
				String s=rs.getString("count");
				System.out.println(String.format("%-10s","注册成功人数: ")+s);
				hm=new HashMap<String, String>();
				hm.put("k", "注册成功人数");
				hm.put("v", s);
				wblist.add(hm);
			}
			
			//小区选择页UV
			pstmt=conn.prepareStatement("select count(*) as count from (select distinct (uid) from "+tname+" where url='/commInfo/nearCommunity') as a;");
			rs=pstmt.executeQuery();
			while(rs.next()){
				String s=rs.getString("count");
				System.out.println(String.format("%-10s","小区选择页UV: ")+s);
				hm=new HashMap<String, String>();
				hm.put("k", "小区选择页UV");
				hm.put("v", s);
				wblist.add(hm);
			}
			
			//认证页UV
			pstmt=conn.prepareStatement("select count(*) as count from (select distinct (uid) from "+tname+" where url='/authApply/invitationCodeAuthApply' or url='authApply/locationAuthApply') as a;");
			rs=pstmt.executeQuery();
			while(rs.next()){
				String s=rs.getString("count");
				System.out.println(String.format("%-10s","认证页UV: ")+s);
				hm=new HashMap<String, String>();
				hm.put("k", "认证页UV");
				hm.put("v", s);
				wblist.add(hm);
			}
			
			//认证成功人数
			dbc=getMongoCollection("UserProfile");
			long cx=dbc.count(new BasicDBObject("accountStatus","setup_userProfile").append("registerDate", new BasicDBObject("$lt",new Date(endtime)).append("$gt", new Date(starttime))));
			System.out.println(String.format("%-10s","认证成功人数: ")+cx);
			hm=new HashMap<String, String>();
			hm.put("k", "认证成功人数");
			hm.put("v", cx);
			wblist.add(hm);
			
			//每个小区UV(分小区日活跃用户数)
			String communityName=null;
			String uv=null;
			pstmt=conn.prepareStatement("select communityName,uv from (select commid,count(distinct uid) as uv from "+tname+" where commid is not null and commid != 'null' and uid is not null group by commid) as a,youlinWeb.communityinfo c where a.commid=c.comid order by uv desc");
			rs=pstmt.executeQuery();
			System.out.println("分小区日活跃用户数: ");
			hm=new HashMap<String, String>();
			hm.put("k", "分小区日活跃用户数如下：");
			hm.put("v", "");
			wblist.add(hm);
			while(rs.next()){
				communityName=rs.getString("communityName");
				uv=rs.getString("uv");
				System.out.println(String.format("%-15s",communityName)+uv);
				hm=new HashMap<String, String>();
				hm.put("k", communityName);
				hm.put("v", uv);
				wblist.add(hm);
			}
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		//输出数据到excel
		makeKeyValueWB(wblist,date);
		
		System.out.println(date+" 统计结果------------------------------------------------");
		System.out.println();
	}
	
	public static DBCollection getMongoCollection(String collection){
		try {
			// 连接到 mongodb 服务
			ServerAddress sa = new ServerAddress("192.168.1.25", 30000);
			MongoCredential mc = MongoCredential.createCredential("youlinapptest_w", "youlinapp", "yL$mongoUsbTest_WR".toCharArray());

			List<ServerAddress> saList = new ArrayList<>();
			List<MongoCredential> mcList = new ArrayList<>();

			saList.add(sa);
			mcList.add(mc);

			MongoClient mongoClient = new MongoClient(saList, mcList);

			DB youlinappDB = mongoClient.getDB("youlinapp");

			DBCollection userProfileTable = youlinappDB.getCollection(collection);
			
			return userProfileTable;

		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
		
	}
	
	public static void makeKeyValueWB(List<Map> list,String wbname) throws IOException
	{

		// 创建Excel的工作书册 Workbook,对应到一个excel文档
		HSSFWorkbook wb = new HSSFWorkbook();
		// 创建Excel的工作sheet,对应到一个excel文档的tab
		// 样式
		HSSFSheet sheet1 = wb.createSheet("sheet1");
		int rowcount=0;
		HSSFRow r=null;
		HSSFCell rv1=null;
		HSSFCell rv2=null;
		for (Map hm : list) {
			r = sheet1.createRow(rowcount++);
			rv1 = r.createCell(0);
			rv2 = r.createCell(1);
			rv1.setCellValue(hm.get("k").toString());
			rv2.setCellValue(hm.get("v").toString());
		}

		FileOutputStream os = new FileOutputStream("f:/"+wbname+".xls");
		wb.write(os);
		os.close();
	}
}
