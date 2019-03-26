package newradj.utils;

import newradj.database.DataDefinition;
import java.io.*;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


public class ScriptToSQL {

    String app;
    String module;
    String entity;
    String field;

    DataDefinition db;
    Properties info;
    String table;
    String column;
            
    int lineNr;
    String err = "";


    public static void main(String[] args) throws Exception {
        String script = "test/classicmodels.script";
        if(args.length>0)
            script = args[0];
        new ScriptToSQL(script);       
    }    

    public ScriptToSQL(String script) throws IOException, SQLException {
        BufferedReader file = new BufferedReader(new FileReader(script));
        String line = file.readLine();
        lineNr = 1;
        while(line!=null){
            interpret(line);
            line = file.readLine();
            lineNr++;
        }
        if(!err.isEmpty()){
            put(err);
            //Rollback
        }
    }

    void interpret(String line) throws SQLException{

        String[] s = line.split("#")[0].trim().split("\\s+");

        if(s[0].equalsIgnoreCase("application")){
            if(s.length>1 && app==null){
                if(s[1]!=null){
                    app = s[1];
                    module = null;
                    table = null;
                    column = null;
                    
                    db = new DataDefinition(s[1]);
                    info = new Properties();
                }
                else error("Application must have a name");
            }
            else error("One application by script");
        }else
            
        if(s[0].equalsIgnoreCase("module")){
            if(!db.isConnected())
                db.connect(info);
            if(s.length>1 && s[1]!=null){
                module = s[1];
                table = null;
                column = null;
            }else error("Module must have a name");
        }else
            
        if(s[0].equalsIgnoreCase("entity")){
//          if(module!=null){
                if(s.length>1 && s[1]!=null){
                    entity = s[1];
                    table = s[1];
                    column = null;
                    String res = db.createTable(table);
                    if(!res.isEmpty())
                        error(res);
                }else
                    error("Entity must have a name");
//            }else error("Module is not defined");
        }else
            
        if(s[0].equalsIgnoreCase("field")){
            if(entity!=null){
                if(s.length>1 && s[1]!=null){
                    field = s[1];
                    column = s[1];
                    String res = db.createColumn(table, column);
                    if(!res.isEmpty())
                        error(res);
                }else error("Field must have a name");
            }else error("Entity must be defined");
        }else

        if(s[0].equalsIgnoreCase("type")){
            if(table!=null && column!=null){
                if(s.length>1 && s[1]!=null){
                    String type = s[1].toUpperCase();
                    if(type.equals("NUMERIC"))
                        type = "DOUBLE";
                    String res = db.setType(table, column, type);
                    if(!res.isEmpty())
                        error(res);
                }else error("Type must have a name");
            }else error("Field must be defined");
        }else
            
        if(module==null && !s[0].isEmpty()){
                if(s.length>1)
                    info.put(s[0], s[1]);
                else
                    info.put(s[0], "");
        }else 

        if(!s[0].isEmpty())
            error("Unknow keyword "+s[0]);

        put(line);
    }
    
    void error(String msg){
        err += "\nERROR on line "+lineNr+": "+msg+".";
    }
    void put(Object msg){
        System.out.println(msg);
    }
}
