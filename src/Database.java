package newradj.database;

import java.sql.*;
import java.util.*;


public class Database {

    String driver = "mysql";
    String host = "localhost";
    int port = 3306;
    String database;
        
    Statement db; 
    Map<String, Map<String, Map<String, String>>> meta;
    
    public Database(String database, String username, String password) throws SQLException{
        this.database = "jdbc:"+driver+"://"+host+":"+port+"/"+database;
        Properties info = new Properties();
        info.put("username", username);
        info.put("password", password);
        connect(info);
    }

    public Database(String database) throws SQLException{
        this.database = "jdbc:"+driver+"://"+host+":"+port+"/"+database;    
    }
    
    public void connect(Properties info) throws SQLException{
        db = DriverManager.getConnection(database, info).createStatement();
    }
    public boolean isConnected(){
        return db!=null;
    }
    public Statement getDb(){
        return db;
    }
    public DatabaseMetaData getMeta(){
        try{
            return db.getConnection().getMetaData();
        }catch(SQLException err){}
        return null;
    }
    
    public List<String> listTables(){
        if(meta==null){
            meta = new TreeMap<>();
            try{
                ResultSet rs = getMeta().getTables(null, null, "%", new String[]{"TABLE"});
                while(rs.next())
                    meta.put(rs.getString(3), null);
                rs.close();
            }catch(SQLException err){}
        }
        return new ArrayList<String>(meta.keySet());
    }
    public List<String> listColumns(String table){
        if(meta==null)
            listTables();
        if(meta.get(table)==null){
            Map<String, Map<String, String>> columns = new TreeMap<>();
            try{
                ResultSet rs = getMeta().getColumns(null, null, table, "%");
                while(rs.next())
                    columns.put(rs.getString(4), null);
                rs.close();
                meta.put(table, columns);
            }catch(SQLException err){}
        }
        return new ArrayList<String>(meta.get(table).keySet());
    }
    
    
    public String toString(){
       return "Database "+database+" "+(isConnected() ? "connected" : "not connected");
    }
    void put(Object msg){
        System.out.println(msg);
    }

    public static void main(String[] args) throws SQLException {
        Database db = new Database("test", "", "");
    }
}
