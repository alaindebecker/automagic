package newradj.utils;

import java.sql.*;
import java.util.Properties;



public class SqlToScript {

    Statement db;
    DatabaseMetaData meta;
    
    public static void main(String[] args) throws SQLException{
        String database = "jdbc:mysql://localhost/classicmodels";
        String username = "";
        String password = "";
        if(args.length>0)
            database = args[0];
        if(args.length>1)
            username = args[1];
        if(args.length>2)
            password = args[2];
        
        new SqlToScript(database, username, password);
    }

    public SqlToScript(String database, String username, String password) throws SQLException {
        Properties info = new Properties();
        info.put("username", username);
        info.put("password", password);
        db = DriverManager.getConnection(database, info).createStatement();
        meta = db.getConnection().getMetaData();
        String model = db.getConnection().getCatalog();
        if(model==null)
            model = db.getConnection().getSchema();
        put("application "+model);
        put("  username "+username);
        put("  password "+password);
        getSchema(model);
    }

    void getSchema(String model){
        put("\n  module "+model);
        try{
            ResultSet tables = meta.getTables(null, null, "%", new String[]{"TABLE"});
            while(tables.next())
                getTable(tables.getString(3));
        }catch(SQLException err){
            err.printStackTrace();
        }        
    }
    void getTable(String table){
        put("\n    entity "+table);
        try{
            ResultSet columns = meta.getColumns(null, null, table, "%");
            while(columns.next())
                getColumn(table, columns.getString(4));
        }catch(SQLException err){
            err.printStackTrace();
        }
    }
    void getColumn(String table, String column){
        put("      field "+column);
        try{
            ResultSet columns = meta.getColumns(null, null, table, column);
            if(columns.next())
                if(!getColumnType(columns.getInt(5)).equalsIgnoreCase("String"))
                    put("        type "+getColumnType(columns.getInt(5)));
        }catch(SQLException err){
            err.printStackTrace();
        }
    }
    String getColumnType(int type){
        switch(type){
            case Types.BIGINT :
            case Types.INTEGER :
            case Types.SMALLINT :
                return "Integer";

            case Types.DECIMAL :
            case Types.DOUBLE :
            case Types.FLOAT :
            case Types.NUMERIC :
            case Types.REAL :
                return "Numeric";

            case Types.CHAR :
            case Types.NCHAR :
            case Types.VARCHAR :
                return "String";

            case Types.LONGNVARCHAR :
            case Types.LONGVARCHAR :
            case Types.BLOB :
            case Types.CLOB :
            case Types.LONGVARBINARY :
            case Types.BINARY :
            case Types.VARBINARY :
                return "Text";

            case Types.BIT :
            case Types.BOOLEAN :
            case Types.TINYINT :
                return "Boolean";

            case Types.DATE :
            case Types.TIME :
            case Types.TIMESTAMP :
            case Types.TIMESTAMP_WITH_TIMEZONE :
            case Types.TIME_WITH_TIMEZONE :
                return "Date";
    
            default:
                return "Unknown "+type;
        }
    }
    
    void put(Object msg){
        System.out.println(msg);
    }    
}
