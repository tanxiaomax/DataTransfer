import java.util.List;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;





public class DB2Tools {
	
	private static Connection conn;

	public static Connection ConnecttoDB( String host,String port,String dbname,
			String user, String password) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException
	{
		Connection con = null;
		if(host == null || port ==null || dbname ==null
				|| user == null || password == null)
		{
			System.out.println("Please input right DB server configuration");
			return con;
		}

		//jdbc:db2://ralbz3191.cloud.dst.ibm.com:60000/DMMSDEV:retrieveMessagesFromServerOnGetMessage=true;
		
		Class.forName("com.ibm.db2.jcc.DB2Driver").newInstance();   
		String url = "jdbc:db2://"+host+":"+port+"/"+dbname; 
	
		try		
		{
			con = DriverManager.getConnection(url, user, password);
		}
		catch(SQLException e)
		{
			System.out.println("A database access error occurs");
			e.printStackTrace();
		}
		
			return con;
		
	}
	
	public static List<String> GetColumnNames(Connection conn, String schemaname, String tablename,String type) throws SQLException
	{
		List<String> columnnames=new ArrayList<String>();
		schemaname = schemaname.toUpperCase();
		tablename = tablename.toUpperCase();
		String getcolumns = null;
		if (type == "All")
		{
			getcolumns = "select COLNAME from syscat.columns where TABSCHEMA = "+
				"'"+schemaname+"'"+" and "+"TABNAME ="+"'" +tablename+"'";
		}
		else
		{
			getcolumns = "select COLNAME from SYSCAT.KEYCOLUSE where TABSCHEMA = "+
					"'"+schemaname+"'"+" and "+"TABNAME ="+"'" +tablename+"'";
			
		}
		if (conn == null || schemaname == null || tablename == null)
		{
			System.out.println("Please input the right parameters");
			return columnnames;
			
		}
		
		try
		{
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(getcolumns);
			System.out.println(getcolumns);
			while(rs.next())
				columnnames.add(rs.getString("COLNAME"));
			stmt.close();
		}
		catch(SQLException e)
		{
			System.out.println("A error occoured when get column names");
			e.printStackTrace();	
		}
	
		if(columnnames.size()==0)
			System.out.println("A error occoured when get column names");
			
		return columnnames;
	}
	
	
	public static ResultSet GetDataFromQuery(Connection conn,String query,String type) throws SQLException
	{
		ResultSet re = null;
		Statement stmt = null;
		if (conn == null)
		{
			System.out.println("invalid connection");
			
			return re;
		}
		
		System.out.print(conn);
		if (type == "source")
		{
			stmt = conn.createStatement();
		}
		else	
		{
			stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE ,ResultSet.CONCUR_UPDATABLE);	 
		}
		
			 
		
		try
		{	
			re=stmt.executeQuery(query);
			
		}
		catch (Exception e)
		{
			System.out.println(query);
			System.out.println("A error occured when excute the query");
			System.out.println(e.getMessage());
		}
		
		
		return re;
		
	}
	
	
//	
//	public boolean TransferDate(Connection source, Connection target,String schema, 
//			String tablename,StringBuffer Query,String Type)
//	{	
//		
//		return false;
//	}
//	
	
	public static int FindID(ResultSet rs, List<String> primarykeys, ArrayList<String> data) throws SQLException
	{
		
		ResultSetMetaData rsmd = rs.getMetaData() ;
		int rsmdcount = rsmd.getColumnCount();
		
	
		
		if(primarykeys.size() > data.size() || primarykeys.size() > rsmdcount )
		{
			System.out.println("Please input the right parameters");
			
			System.out.println("primarykeys :"+primarykeys.size()+"data:"+data.size()
					+"rs:"+rsmdcount);
			return 0;
		}
		
		while(rs.next())
		{
			int flag = 0;
			for ( int i = 0 ; i < primarykeys.size(); i++)
			{
				
				System.out.println((String)rs.getString(primarykeys.get(i)));
				
				
				
				if (data.contains(rs.getString(primarykeys.get(i)).trim()))
				{
					flag++;			
				}
		
				if (flag == primarykeys.size())
				{
					return rs.getRow();
		
				}
			}
			
			
		}
		
		return 0;
	
	}
	
	
	public static ArrayList<String> GetColumnsFromQuery(String query)
	{
		query = query.trim();
		ArrayList<String> columnnames = new ArrayList<String>();
		
		int position = 0;
		if (!query.contains("select") || !query.contains("from"))
		{
			System.out.println("Please input the right SQL");
			return columnnames;
		}
		
		
		position = query.indexOf("from");
		
		
		String columns = query.substring(6, position);
		
		String[] temp = columns.split(",");
		for (int i = 0;i < temp.length; i++ )
		{
			
			if (temp[i].toUpperCase().contains("AS"))
			{
				String[] temp2 = temp[i].toUpperCase().split("AS");
				if (temp2.length > 2)
				{
					System.out.println("Please input the right SQL");
					return null;
				}
				else
				{
					columnnames.add(temp2[1].trim());
				}
			}
			else
			{
				columnnames.add(temp[i].trim());
			}
		}
			
		return columnnames;
	}
	
	

	
	
	public static void main(String args[]) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException
	{
		 Connection conn1 = null;
		 Connection conn2 = null;
		 conn1 = DB2Tools.ConnecttoDB("ralbz001118.raleigh.ibm.com","60000","DMMSUAT"
				,"ecmdmms","z45kcBwN");
		 conn2 = DB2Tools.ConnecttoDB("ralbz3191.cloud.dst.ibm.com","60000","DMMSDEV"
					,"ecmdmms","gL786PQ9");

//		 Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
//		
//		 
//		 ResultSet re=stmt.executeQuery("select A.start_date from dmmscog.reports_validation_2 A JOIN dmmscog.reports_validation_2 B ON A.START_DATE = B.START_DATE");
//		 while(re.next())
//		 {
//			 System.out.println(re.getString(1));
//		 }
//		 
//		 stmt.close();
		 
		 //List<String> columnnames = DB2Tools.GetColumnNames(conn,"dmmscog","ecm_raw","Primary");
		 
		 String test1 = "select  DISTINCT 'Inventory' AS REPORT_TYPE,'M' AS TIME_INTERVAL,'2012-07-01' AS START_DATE,0 AS IS_VISIBLE,0 AS IS_VALID,4 AS TIME_INTERVAL_ORDER,'Monthly' AS TIME_INTERVAL_TYPE from DMMSCOG.REPORTS_VALIDATION_2";
		 String test2 = "select ROWID,CDATE,REP_ID,URL,REP_GROUP,REP_SUB_GROUP,LOCALE,NAME_IN_CONTACT_MODULE,COUNTRY,LINKEDIN,TWITTER,XING,REP_CNUM,WEEK_FLAG,UPDATED_TS,SKYPE,FACEBOOK,VIADEO,SN1,SN2,SYNKEY from dmmscog.ECM_RAW";
		 String test3 = "select REPORT_TYPE,'T' AS TIME_INTERVAL,START_DATE,IS_VISIBLE,0 AS IS_VALID from dmmscog.reports_validation_2 where START_DATE = '2012-07-01' and REPORT_TYPE ='Scorecard'";
		 String test4 = "select REPORT_TYPE, TIME_INTERVAL,START_DATE,IS_VISIBLE, IS_VALID from dmmscog.reports_validation_2";
		 TransferData test = new InsertData(conn2,conn1,"DB2INST1","ECM_RAW_TEST",test2);
		// System.out.print(columnnames.size());
		 try
		 {
			 test.Transerfer();
		 }
		 catch(Exception e)
		 {
			 System.out.println(e.getMessage());
		 }finally
		 {
			 conn1.close();
			 conn2.close();
		 }
		 
		 
		 
		 
		 	
		 //TransferData td = new InsertData(conn,conn,"dmmscog","ecm_raw","select aaa as abc, bbb, ccc from A");
		 
//		 for (int i =0;i < columnnames.size();i++)
//		 {
//			 System.out.println(columnnames.get(i));
//		 }
//		 ArrayList<String> temp = GetColumnsFromQuery("select aaa as abc, bbb, ccc from A");
//		 for (int i =0;i<temp.size();i++)
//		 {
//			 System.out.print(GetColumnsFromQuery("select aaa as abc, bbb, ccc from A").get(i));
//			 
//		 }
//		 
		
	
	}
	
	
}
