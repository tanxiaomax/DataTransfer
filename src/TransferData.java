import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;


public abstract class TransferData {
	
	protected Connection source;
	protected Connection target;	
	protected String schema;
	protected String tablename;
	protected String query;
	
	

	
	public TransferData(Connection source, Connection target,String schema, 
			String tablename,String query) throws SQLException
	{
		this.source = source;
		this.target = target;
		this.schema = schema;
		this.tablename = tablename;
		this.query = query;
		
		//source.setAutoCommit(false);
		target.setAutoCommit(false);
	}
	
	public abstract boolean Transerfer() throws SQLException;
	
	


}
