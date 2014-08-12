
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;


public class InsertData extends TransferData {

	public InsertData(Connection source, Connection target, String schema,
			String tablename, String query) throws SQLException {
		super(source, target, schema, tablename, query);
		
	}



	@Override
	public boolean Transerfer() throws SQLException {
		// TODO Auto-generated method stub
		
		String insertsql = null;
		
	    ArrayList<String> sourcolnames = null;	    
	    ResultSet soudata = null;
	    ResultSet tardata = null;
	    ArrayList<String> temp = new ArrayList<String>();
	    
	    
	    sourcolnames = DB2Tools.GetColumnsFromQuery(this.query);
	    soudata = DB2Tools.GetDataFromQuery(this.source, this.query,"source");

	    
	    
	    
	    ResultSetMetaData soursmd = soudata.getMetaData() ;
	    int soudatacount = soursmd.getColumnCount();
	    insertsql = "insert into "+schema+"."+tablename+" (";
		for (int i =0; i < sourcolnames.size();i++)
    	{	
    		insertsql = insertsql + " "+sourcolnames.get(i);
    		if(i ==  (sourcolnames.size() - 1))
    		{
    			insertsql+=") ";
    		}
    		else
    		{
    			insertsql+=",";
    		}
    						
    	}
		insertsql = insertsql + "VALUES" + " (";
		for (int i =0; i < sourcolnames.size();i++)
		{
			
		    insertsql = insertsql + "? ";
			if(i ==  (sourcolnames.size() -1))
    		{
    			insertsql+=") ";
    		}
    		else
    		{
    			insertsql+=",";
    		}
			
		}
		
		
		System.out.print(insertsql);
		PreparedStatement pstmt = target.prepareStatement(insertsql);
		    
	    
	   
	    while (soudata.next())
	    {
	    	try
			{
	    		for (int i =0; i < sourcolnames.size();i++)
	    		{
	    			pstmt.setString(i + 1, soudata.getString(sourcolnames.get(i)));
	    		}
	    		pstmt.execute();
	    		
			}
			catch(Exception e)
			{
				System.out.println("A error has occured during the insert opration");
				System.out.println(e.getMessage());
			}
   	
	    }
	    
	    target.commit();
 		
		return false;
	}
	
	
	
	

}
