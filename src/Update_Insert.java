import java.util.List;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;


public class Update_Insert extends TransferData {
	
	public  Update_Insert(Connection source, Connection target, String schema,
			String tablename, String query) throws SQLException {
		super(source, target, schema, tablename, query);
		

		
	}

	@Override
	public boolean Transerfer() throws SQLException  {
		int updaterownumber = 0;
	    List<String> primarykeys  = null;
	    ArrayList<String> tarcolnames  = null;
	    ArrayList<String> sourcolnames = null;	    
	    ResultSet soudata = null;
	    ResultSet tardata = null;
	    ArrayList<String> temp = new ArrayList<String>();
	    
	  
	    primarykeys =  DB2Tools.GetColumnNames(this.target, this.schema, this.tablename,"PRIMARY");
	    sourcolnames = DB2Tools.GetColumnsFromQuery(this.query);
	    tarcolnames = DB2Tools.GetColumnsFromQuery(this.query);
	    soudata = DB2Tools.GetDataFromQuery(this.source, this.query,"source");
	    tardata = DB2Tools.GetDataFromQuery(this.target, "select * from " + this.schema+"."+this.tablename,"target");
	    
	    ResultSetMetaData soursmd = soudata.getMetaData() ; 
	    int soudatacount = soursmd.getColumnCount();
	    
	  
	    
	    while (soudata.next())
	    {
	    	for (int i =1; i <= soudatacount;i++)
	    	{
	    		temp.add(soudata.getString(i).trim());
	    	}
	    	
	    	
	    	if(temp.contains("W"))
	    	{
	    		System.out.print("contain");
	    	}
	    	
	    	tardata.beforeFirst();
	    	updaterownumber = DB2Tools.FindID(tardata, primarykeys,temp);
	    	if (updaterownumber != 0)
	    	{
		    	tardata.absolute(updaterownumber);
		    
		    	for(int i = 0; i < sourcolnames.size() ; i++  )
		    	{
		    		if(!primarykeys.contains(sourcolnames.get(i).trim()))
		    		{

		    			try
		    			{
		    				tardata.updateObject(sourcolnames.get(i), soudata.getString(sourcolnames.get(i)));
			    			tardata.updateRow();
		    			}
		    			catch(Exception e)
		    			{
		    				System.out.println(e.getMessage());
		    			}
		    			
		    		}
	
		    	}
	    	}
	    	
	    	else
	    	{
	    		tardata.moveToInsertRow();
	    		try
	    		{
		    		for(int i = 0; i < sourcolnames.size() ; i++  )
		    		{		    	
		    			System.out.println(soudata.getString(sourcolnames.get(i)));
		    			tardata.updateObject(sourcolnames.get(i), soudata.getString(sourcolnames.get(i)));	
		    		}
		    		tardata.insertRow();
	    		}
	    		catch(Exception e)
    			{
    				System.out.println("A error has occured during the insert opration");
    				System.out.println(e.getMessage());
    			}
	    		
	    		
	    		tardata.moveToCurrentRow();
	    	}
		    	
	    		
		    	System.out.println(updaterownumber);
		    	temp.clear();
	    }
	    
	    target.commit();
   
	    System.out.println("Update");
	    return true;
		
		
	}

}
