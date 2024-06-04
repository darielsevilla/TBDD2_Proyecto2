
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
/**
 *
 * @author darie
 */
public class Admin {

    private String name_postgre, name_sqlserver;
    private String puerto_postgre, puerto_sqlserver;
    private String user_postgre, user_sqlserver;
    private String pass_postgre, pass_sqlserver;
    private ArrayList<String> tablasReplicadas;
    private ArrayList<String> tablasSinReplicar;
    private int cual;
    private Connection postgre, sqlserver;

    public Admin(String name_postgre, String name_sqlserver, String puerto_postgre, String puerto_sqlserver, String user_postgre, String user_sqlserver, String pass_postgre, String pass_sqlserver, int cual) {

        this.name_postgre = name_postgre;
        this.name_sqlserver = name_sqlserver;
        this.puerto_postgre = puerto_postgre;
        this.puerto_sqlserver = puerto_sqlserver;
        this.user_postgre = user_postgre;
        this.user_sqlserver = user_sqlserver;
        this.pass_postgre = pass_postgre;
        this.pass_sqlserver = pass_sqlserver;
        this.cual = cual;
        tablasSinReplicar = new ArrayList<>();
        tablasReplicadas = new ArrayList<>();
        //credenciales postgres
        //username = postgres
        //password = Emilio2606
        //cual == 1 -> sqlserver a postgre
        //cual == 2 -> postgre a sqlserver
    }

    public boolean connect() {
        try {

            postgre = DriverManager.getConnection("jdbc:postgresql://localhost:" + puerto_postgre + "/" + name_postgre, user_postgre, pass_postgre);
            //sqlserver = DriverManager.getConnection("jdbc:sqlserver://localhost;encrypt=true;databaseName="+name_sqlserver+";user="+user_sqlserver+";password="+pass_sqlserver);
            //sqlserver = DriverManager.getConnection("jdbc:sqlserver://localhost:1433;databaseName="+name_sqlserver);
            //String connectionUrl ="jdbc:sqlserver://localhost"+puerto_sqlserver+";database="+name_sqlserver+";user=" + user_sqlserver+ ";password="+pass_sqlserver+";encrypt=true;"+ "trustServerCertificate=false;" + "loginTimeout=30;";
            sqlserver = DriverManager.getConnection("jdbc:sqlserver://localhost:" + puerto_sqlserver + ";database=" + name_sqlserver + ";user=" + user_sqlserver + ";password=" + pass_sqlserver + ";encrypt=true;" + "trustServerCertificate=true;" + "loginTimeout=30;");
            loadTablas();
            System.out.println(tablasSinReplicar.get(0));
            printAtributos(getAtributos_sqlserver(tablasSinReplicar.get(0)));
            tablasReplicadas.add(tablasSinReplicar.get(0));
            replicarServerToPostgre();
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }

    }

    public void loadTablas() {
        if (cual == 1) {
            //cargar nombres de tablas de sqlserver en postgre
            try {
                ResultSet results = sqlserver.createStatement().executeQuery("select TABLE_SCHEMA, TABLE_NAME from INFORMATION_SCHEMA.TABLES");
                while (results.next()) {
                    tablasSinReplicar.add(results.getString("TABLE_SCHEMA") + "." + results.getString("TABLE_NAME"));

                }
            } catch (Exception e) {

            }
        } else {
            //cargar nombres de tablas de postre en sqlserver

        }
    }

    public ArrayList<Atribute> getAtributos_sqlserver(String table) {
        ArrayList<Atribute> atributos = new ArrayList<>();
        //primera query busca los atributos y devuelve el atributo, tipo, lengthMaxima, 
        String queryAtributos = "SELECT \n"
                + "    c.name 'Atributo',\n"
                + "    t.Name 'Tipo',\n"
                + "    c.max_length,\n"
                + "	t.system_type_id,\n"
                + "       t.is_user_defined,\n"
                + "    c.precision ,\n"
                + "    c.is_nullable,\n"
                + "    ISNULL(i.is_primary_key, 0) 'Primary Key'\n"
                + "FROM    \n"
                + "    sys.columns c\n"
                + "INNER JOIN \n"
                + "    sys.types t ON c.user_type_id = t.user_type_id\n"
                + "LEFT OUTER JOIN \n"
                + "    sys.index_columns ic ON ic.object_id = c.object_id AND ic.column_id = c.column_id\n"
                + "LEFT OUTER JOIN \n"
                + "    sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id\n"
                + "WHERE\n"
                + "    c.object_id = OBJECT_ID('" + table + "')";

        try {
            //esto consigue los atributos iniciales
            ResultSet atributosTabla = sqlserver.createStatement().executeQuery(queryAtributos);

            while (atributosTabla.next()) {
                String nombre = atributosTabla.getString("Atributo");
                String tipo = atributosTabla.getString("Tipo");
                int size = atributosTabla.getInt("max_length");
                int precision = atributosTabla.getInt("precision");
                
                String alias = null;
                boolean isNull = false;
                boolean isPK = false;
                boolean isFK = false;
                String reference = null;
                if (atributosTabla.getInt("is_nullable") == 1) {
                    isNull = true;
                }
                if (atributosTabla.getInt("Primary Key") == 1) {
                    isPK = true;
                }

                //saca el tipo de dato de verdad si es alias
                if (atributosTabla.getInt("is_user_defined") == 1) {
                    String query2 = "select * from sys.types\n"
                            + "where system_type_id = user_type_id and system_type_id =" + atributosTabla.getInt("system_type_id") + ";";
                    ResultSet realName = sqlserver.createStatement().executeQuery(query2);
                    realName.next();
                    alias = tipo;
                    tipo = realName.getString("name");
                }
                if(tipo.contains("char")){
                    size /= 2;
                }
                //revisa si es foreign key
                String llaveForanea = "SELECT f.name AS foreign_key_name,\n"
                        + "    OBJECT_NAME(f.parent_object_id) AS table_name,\n"
                        + "    COL_NAME(fc.parent_object_id, fc.parent_column_id) AS constraint_column_name,\n"
                        + "    OBJECT_NAME(f.referenced_object_id) AS referenced_object,\n"
                        + "    COL_NAME(fc.referenced_object_id, fc.referenced_column_id) AS referenced_column_name\n"
                        + "FROM sys.foreign_keys AS f\n"
                        + "INNER JOIN sys.foreign_key_columns AS fc\n"
                        + "    ON f.object_id = fc.constraint_object_id\n"
                        + "WHERE f.parent_object_id = OBJECT_ID('HumanResources.Employee') AND COL_NAME(fc.parent_object_id, fc.parent_column_id) = '" + nombre + "';";
                ResultSet foranea = sqlserver.createStatement().executeQuery(llaveForanea);
                String rCol = null;
                if (foranea.next()) {
                    isFK = true;
                    reference = foranea.getString("table_name");
                    rCol = foranea.getString("referenced_column_name");
                }
                atributos.add(new Atribute(nombre, isFK, isPK, tipo, reference, size, isNull, alias, precision, rCol));

            }
        } catch (Exception e) {

        }

        return atributos;
    }

    public ArrayList<Atribute> getAtributos_postgre(String table) {
        ArrayList<Atribute> atributos = new ArrayList<>();

        return atributos;
    }

    public boolean replicarPostgreToServer() {
        return true;
    }

    //replicación sql server a postgre
    public boolean replicarServerToPostgre() {
        try {
            ArrayList<Atribute> atributos;
            ResultSet existe;
            for (int i = 0; i < tablasReplicadas.size(); i++) {
                //verificar si la tabla existe
                atributos = getAtributos_sqlserver(tablasReplicadas.get(i));
                String schema = "public";
                String name = "";
                System.out.println(tablasReplicadas.get(i));
                String[] array = tablasReplicadas.get(i).replace('.','-').split("-");
               
                //separar el nombre del esquema 
                if (array.length > 1) {
                    //verificar si el esquema existe

                    schema = array[0];
                    name = array[1];
                    String schemaqueue = "SELECT schema_name FROM information_schema.schemata WHERE schema_name ='" + schema.toLowerCase() + "';";
                    existe = postgre.createStatement().executeQuery(schemaqueue);
                    if (!existe.next()) {
                        //si no existe se crea
                        postgre.createStatement().execute("create schema " + schema);
                    }
                } else {
                    name = array[0];
                }
                //ahora verifica si la tabla existe
                existe = postgre.createStatement().executeQuery("select table_name from information_schema.tables where table_name = '" + name + "' and table_schema = '" + schema + "'");
                //si la tabla no existe la crea
                if (!existe.next()) {
                    replicarEstructura1(tablasReplicadas.get(i), atributos);
                }
            }

        } catch (SQLException ex) {
            Logger.getLogger(Admin.class.getName()).log(Level.SEVERE, null, ex);
        }
        return true;
    }

    //replicación de la estructura de la tabla en si (server -> postgre)
    private boolean replicarEstructura1(String nombre, ArrayList<Atribute> atributos) {
        String create = "create table " + nombre + "(";
        ResultSet existe;
        String query = "";
        boolean tampered = false;
        //el name es el data type pero me confundi asi que asi se queda
        try {
            for (int i = 0; i < atributos.size(); i++) {
                String name = atributos.get(i).getDataType();
                String alias = atributos.get(i).getAlias();
                if(alias != null){
                    String queueAlias = "select typname from pg_type where typname = '" + alias.toLowerCase() + "'";
                    System.out.println(alias);
                    existe = postgre.createStatement().executeQuery(queueAlias);
                    //si el alias ya existe es q o es un tipo de dato en postgre o ya fue definido
                    if (existe.next()) {
                       name = alias;
                       alias = null;
                        System.out.println("entra");
                  
                    }
                }
                 
                if(name.equals("tinyint")){
                    name = "smallint";
                    tampered = true;
                }else if(name.equals("smallmoney")){
                    name = "money";
                    tampered = true;
                }else if(name.equals("smalldatetime")){
                    name = "timestamp(0)";
                    tampered = true;
                }else if(name.equals("datetime")){
                    name = "timestamp(3)";
                    tampered = true;
                }else if(name.equals("datetime2")){
                    name = "timestamp(3)";
                    tampered = true;
                }else if(name.equals("datetimeoffset")){
                    name = "timestamp(3) with time zone";
                    tampered = true;
                }else if(name.equals("binary") || name.equals("varbinary") || name.equals("image") || name.equals("varbinary(max)")){
                    name = "bytea";
                    tampered = true;
                }else if(name.equals("ntext") || name.equals("nvarchar(max)") || name.equals("varchar(max)")){
                    name = "text";
                    tampered = true;
                }else if(name.equals("uniqueidentifier")){
                    name = "char(16)";
                    tampered = true;
                }else if(name.equals("hierarchyid")){
                    name = "varchar(4000)";
                    tampered = true;
                }else if(name.equals("rowversion")){
                    name = "timestamp(3)";
                    tampered = true;
                }else if(name.equals("nvarchar")){
                    name = "varchar";
                }
                if(alias != null){
                    query = "create domain " + alias + " as " + name;
                    if(!tampered && name.contains("char")){
                        query += "(" + atributos.get(i).getSize()+")";
                        tampered = true;
                    }
                
                    postgre.createStatement().execute(query);
                    
                    name = alias;
                }
                
                create += atributos.get(i).getName() + " " + name;
                if(name.contains("char") && !tampered){
                    create += "(" + atributos.get(i).getSize()+")";
                }
                if(!atributos.get(i).isIsNull()){
                    create += " not null";
                }
                if(atributos.get(i).isPrimaryKey()){
                    create += " primary key";
                }
                
                if(atributos.get(i).isForeignKey()){
                    create += " foreign key references " + atributos.get(i).getReference() + "("+atributos.get(i).getColumnafk()+")";
                }
                if(i != atributos.size()-1){
                    create += ",\n";
                    query = "";
                }else{
                    create += "\n";
                }
                
                query = "";
                tampered = false;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        create += ")";
        
        try{
           postgre.createStatement().execute(create);
            return true;
        }catch(Exception e){
           
            return false;
        }
    }
    //replicar datos (server -> postgre)
    private boolean replicarDatos1(String nombre, ArrayList<Atribute> atributos){
        return true;
    }
    
    public void printSinReplicar() {
        for (int i = 0; i < tablasSinReplicar.size(); i++) {
            System.out.println(tablasSinReplicar.get(i));
        }
    }

    public void printAtributos(ArrayList<Atribute> atributos) {
        for (int i = 0; i < atributos.size(); i++) {
            System.out.println("-------------------------------------------");
            System.out.println(atributos.get(i).toString());
            System.out.println("-------------------------------------------");
        }

    }

}
