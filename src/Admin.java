
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
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
    private String instancia_postgre, instancia_sql;
    private ArrayList<String> tablasReplicadas;
    private ArrayList<String> tablasSinReplicar;
    private int cual;
    private Connection postgre, sqlserver;

    public Admin(String instancia_postgre, String instancia_sql, String name_postgre, String name_sqlserver, String puerto_postgre, String puerto_sqlserver, String user_postgre, String user_sqlserver, String pass_postgre, String pass_sqlserver, int cual) {

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
        this.instancia_postgre = instancia_postgre;
        this.instancia_sql = instancia_sql;
        //credenciales postgres
        //username = postgres
        //password = Emilio2606
        //cual == 1 -> sqlserver a postgre
        //cual == 2 -> postgre a sqlserver
    }

    public boolean connect() {
        try {
            //postgre = DriverManager.getConnection("jdbc:postgresql://localhost:" + puerto_postgre + "/" + name_postgre, user_postgre, pass_postgre);
            //sqlserver = DriverManager.getConnection("jdbc:sqlserver://localhost:" + puerto_sqlserver + ";database=" + name_sqlserver + ";user=" + user_sqlserver + ";password=" + pass_sqlserver + ";encrypt=true;" + "trustServerCertificate=true;" + "loginTimeout=30;");
            postgre = DriverManager.getConnection("jdbc:" + instancia_postgre+":" + puerto_postgre + "/" + name_postgre, user_postgre, pass_postgre);
            sqlserver = DriverManager.getConnection("jdbc:"+instancia_sql+":" + puerto_sqlserver + ";database=" + name_sqlserver + ";user=" + user_sqlserver + ";password=" + pass_sqlserver + ";encrypt=true;" + "trustServerCertificate=true;" + "loginTimeout=30;");
            loadTablas();

            //this.printSinReplicar();
            //System.out.println(this.tablasSinReplicar.get(8));
            //this.tablasReplicadas.add(this.tablasSinReplicar.get(8));
            //System.out.println(this.replicarServerToPostgre(null));
            //tablasReplicadas.add(tablasSinReplicar.get(0));
            //replicarServerToPostgre();
            return true;
        } catch (Exception e) {
            
            return false;
        }

    }

    public boolean test(String instancia, String puerto, String base, String usuario, String pw, int type) {
        try {
            if (type == 1) {//sqlserver
                DriverManager.getConnection("jdbc:" + instancia + ":" + puerto + ";database=" + base + ";user=" + usuario + ";password=" + pw + ";encrypt=true;" + "trustServerCertificate=true;" + "loginTimeout=30;");
            } else {
                DriverManager.getConnection("jdbc:" + instancia + ":" + puerto + "/" + base, usuario, pw);//postgres
            }
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

                    if (!results.getString("TABLE_SCHEMA").equals("sys") && !results.getString("TABLE_SCHEMA").equals("cdc") && !results.getString("TABLE_NAME").equals("replicationlog") && !results.getString("TABLE_NAME").equals("systranschemas")) {
                        tablasSinReplicar.add(results.getString("TABLE_SCHEMA") + "." + results.getString("TABLE_NAME"));
                    }
                }
            } catch (Exception e) {

            }
        } else {
            try {
                ResultSet results = postgre.createStatement().executeQuery("select table_name, table_schema from information_schema.tables where table_schema != 'information_schema' and table_schema != 'pg_catalog'");
                while (results.next()) {
                    tablasSinReplicar.add(results.getString("table_schema") + "." + results.getString("table_name"));

                }
            } catch (Exception e) {

            }
            //cargar nombres de tablas de postre en sqlserver
            
            this.printAtributos(this.getAtributos_postgre(tablasSinReplicar.get(2)));
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
                if (tipo.contains("char")) {
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
                while (foranea.next()) {
                    isFK = true;
                    reference = foranea.getString("table_name");
                    rCol = foranea.getString("referenced_column_name");
                    boolean isThere = false;
                    for (String str : this.tablasReplicadas) {

                        if (str.equals(reference)) {
                            isThere = true;
                        }
                    }
                    if (!isThere) {
                        return null;
                    }
                }
                atributos.add(new Atribute(nombre, isFK, isPK, tipo, reference, size, isNull, alias, precision, rCol));

            }
        } catch (Exception e) {

        }

        return atributos;
    }

    public ArrayList<String> getTablasReplicadas() {
        return tablasReplicadas;
    }

    public int getCual() {
        return cual;
    }

    public void setCual(int cual) {
        this.cual = cual;
    }

    public void setTablasReplicadas(ArrayList<String> tablasReplicadas) {
        this.tablasReplicadas = tablasReplicadas;
    }

    public ArrayList<String> getTablasSinReplicar() {
        return tablasSinReplicar;
    }

    public void setTablasSinReplicar(ArrayList<String> tablasSinReplicar) {
        this.tablasSinReplicar = tablasSinReplicar;
    }

    public ArrayList<Atribute> getAtributos_postgre(String table) {
        ArrayList<Atribute> atributos = new ArrayList<>();
        String schema = "public";
        String tabla = table;
        String[] split = table.replace('.', '-').split("-");
        if (split.length == 2) {
            schema = split[0];
            tabla = split[1];
        }
        String pKeys = "SELECT c.column_name, c.data_type\n"
                + "FROM information_schema.table_constraints tc \n"
                + "JOIN information_schema.constraint_column_usage AS ccu USING (constraint_schema, constraint_name) \n"
                + "JOIN information_schema.columns AS c ON c.table_schema = tc.constraint_schema\n"
                + "  AND tc.table_name = c.table_name AND ccu.column_name = c.column_name\n"
                + "WHERE constraint_type = 'PRIMARY KEY' and tc.table_name = '" + tabla + "' and tc.table_schema = '" + schema + "';";

        String attQuery = "SELECT *\n"
                + "  FROM information_schema.columns\n"
                + " WHERE table_schema = '" + schema + "'\n"
                + "   AND table_name   = '" + tabla + "';";
        try {
            ResultSet keys = postgre.createStatement().executeQuery(pKeys);
            String pKName = "";
            if (keys.next()) {
                pKName = keys.getString("column_name");
            }

            ResultSet rs = postgre.createStatement().executeQuery(attQuery);
            //boolean foreignKey, String reference,String alias, String columnafk)
            while (rs.next()) {
                String name = rs.getString("column_name");
                String dataType = rs.getString("udt_name");
                int size = rs.getInt("character_maximum_length");
                boolean isNull = false;
                boolean pkey = false;
                boolean fkey = false;
                String reference = null;
                String columnafk = null;
                String alias = null;
                int precision = -1;
                //checkear null
                String nul = rs.getString("is_nullable");
                if (nul.equals("YES")) {
                    isNull = true;
                }

                //precision
                if (rs.getString("numeric_precision") != null) {
                    precision = rs.getInt("numeric_precision");
                }
                //primary key
                if (name.equals(pKName)) {
                    pkey = true;
                }

                //foreign key
                String sql = "SELECT\n"
                        + "    tc.table_schema, \n"
                        + "    tc.constraint_name, \n"
                        + "    tc.table_name, \n"
                        + "    kcu.column_name, \n"
                        + "    ccu.table_schema AS foreign_table_schema,\n"
                        + "    ccu.table_name AS foreign_table_name,\n"
                        + "    ccu.column_name AS foreign_column_name \n"
                        + "FROM information_schema.table_constraints AS tc \n"
                        + "JOIN information_schema.key_column_usage AS kcu\n"
                        + "    ON tc.constraint_name = kcu.constraint_name\n"
                        + "    AND tc.table_schema = kcu.table_schema\n"
                        + "JOIN information_schema.constraint_column_usage AS ccu\n"
                        + "    ON ccu.constraint_name = tc.constraint_name\n"
                        + "WHERE tc.constraint_type = 'FOREIGN KEY'\n"
                        + "    AND tc.table_schema='" + schema + "'\n"
                        + "    AND tc.table_name='" + tabla + "'\n"
                        + "	AND kcu.column_name = '" + name + "';";

                ResultSet fk = postgre.createStatement().executeQuery(sql);
                if (fk.next()) {
                    fkey = true;
                    reference = fk.getString("foreign_table_name");
                    columnafk = fk.getString("foreign_column_name");
                }

                //chequeo de alias
                ResultSet domain = postgre.createStatement().executeQuery("select * from information_schema.domains where domain_name = '" + name + "';");
                if (domain.next()) {
                    alias = name;
                    name = domain.getString("udt_name");
                }
                Atribute atributo = new Atribute(name, fkey, pkey, dataType, reference, size, isNull, alias, precision, columnafk);
                atributos.add(atributo);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return atributos;
    }

    public boolean replicarPostgreToServer(Date date) {

        return true;
    }

    public boolean replicarEstructura2(String nombre, ArrayList<Atribute> atributos) {

        String[] split = nombre.replace('.', '-').split("-");
        if (split.length == 2) {
            if (split[0].equals("public")) {
                System.out.println(split[0]);
                System.out.println(split[1]);
                nombre = split[1];
            }
        }
        String create = "create table " + nombre + "(";
        ResultSet existe;
        String query = "";
        String end = "";
        boolean tampered = false;
        //el name es el data type pero me confundi asi que asi se queda
        try {
            for (int i = 0; i < atributos.size(); i++) {
                String name = atributos.get(i).getDataType();
                String alias = atributos.get(i).getAlias();
                if (alias != null) {
                    String queueAlias = "select typname from pg_type where typname = '" + alias.toLowerCase() + "'";
                    System.out.println(alias);
                    existe = postgre.createStatement().executeQuery(queueAlias);
                    //si el alias ya existe es q o es un tipo de dato en postgre o ya fue definido
                    if (existe.next()) {
                        name = alias;
                        alias = null;

                    }
                }

                if (name.equals("int4")) {
                    name = "int";
                    tampered = true;
                } else if (name.equals("float8")) {
                    name = "float";
                    tampered = true;
                }

                if (alias != null) {
                    query = "create domain " + alias + " as " + name;
                    if (!tampered && name.contains("char")) {
                        query += "(" + atributos.get(i).getSize() + ")";
                        tampered = true;
                    }

                    sqlserver.createStatement().execute(query);

                    name = alias;
                }

                create += atributos.get(i).getName() + " " + name;
                if (name.contains("char") && !tampered) {
                    if (atributos.get(i).getSize() != 0) {
                        create += "(" + atributos.get(i).getSize() + ")";
                    } else {
                        create += "(max)";
                    }
                }
                if (!atributos.get(i).isIsNull()) {
                    create += " not null";
                }
                if (atributos.get(i).isPrimaryKey()) {
                    create += " primary key";
                }

                if (atributos.get(i).isForeignKey()) {
                    end += " foreign key (" + nombre + ") references " + atributos.get(i).getReference() + "(" + atributos.get(i).getColumnafk() + ")";

                }
                if (i != atributos.size() - 1) {
                    create += ",\n";
                    query = "";
                } else if (!end.equals("")) {
                    create += ",\n";
                    create += end;
                } else {
                    create += "\n";
                }

                query = "";
                tampered = false;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        create += ")";

        try {
            System.out.println(create);
            sqlserver.createStatement().execute(create);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    //replicación sql server a postgre
    public boolean replicarServerToPostgre(Date date) {
        try {
            ArrayList<Atribute> atributos;
            ResultSet existe;
            for (int i = 0; i < tablasReplicadas.size(); i++) {
                //verificar si la tabla existe
                atributos = getAtributos_sqlserver(tablasReplicadas.get(i));
                if (atributos == null) {

                    return false;
                }
                String schema = "public";
                String name = "";
                System.out.println(tablasReplicadas.get(i));
                String[] array = tablasReplicadas.get(i).replace('.', '-').split("-");

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
                    boolean replicate = replicarEstructura1(tablasReplicadas.get(i), atributos);

                }

                //replica datos
                return replicarDatos1(schema, name, atributos, date);
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
        String end = "";
        boolean tampered = false;
        //el name es el data type pero me confundi asi que asi se queda
        try {
            for (int i = 0; i < atributos.size(); i++) {
                String name = atributos.get(i).getDataType();
                String alias = atributos.get(i).getAlias();
                if (alias != null) {
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

                if (name.equals("tinyint")) {
                    name = "smallint";
                    tampered = true;
                } else if (name.equals("smallmoney")) {
                    name = "money";
                    tampered = true;
                } else if (name.equals("smalldatetime")) {
                    name = "timestamp(0)";
                    tampered = true;
                } else if (name.equals("datetime")) {
                    name = "timestamp(3)";
                    tampered = true;
                } else if (name.equals("datetime2")) {
                    name = "timestamp(3)";
                    tampered = true;
                } else if (name.equals("datetimeoffset")) {
                    name = "timestamp(3) with time zone";
                    tampered = true;
                } else if (name.equals("binary") || name.equals("varbinary") || name.equals("image") || name.equals("varbinary(max)")) {
                    name = "bytea";
                    tampered = true;
                } else if (name.equals("ntext") || name.equals("nvarchar(max)") || name.equals("varchar(max)")) {
                    name = "text";
                    tampered = true;
                } else if (name.equals("uniqueidentifier")) {
                    name = "char(16)";
                    tampered = true;
                } else if (name.equals("hierarchyid")) {
                    name = "varchar(4000)";
                    tampered = true;
                } else if (name.equals("rowversion")) {
                    name = "timestamp(3)";
                    tampered = true;
                } else if (name.equals("nvarchar")) {
                    name = "varchar";
                }
                if (alias != null) {
                    query = "create domain " + alias + " as " + name;
                    if (!tampered && name.contains("char")) {
                        query += "(" + atributos.get(i).getSize() + ")";
                        tampered = true;
                    }

                    postgre.createStatement().execute(query);

                    name = alias;
                }

                create += atributos.get(i).getName() + " " + name;
                if (name.contains("char") && !tampered) {
                    create += "(" + atributos.get(i).getSize() + ")";
                }
                if (!atributos.get(i).isIsNull()) {
                    create += " not null";
                }
                if (atributos.get(i).isPrimaryKey()) {
                    create += " primary key";
                }

                if (atributos.get(i).isForeignKey()) {
                    end += " foreign key (" + nombre + ") references " + atributos.get(i).getReference() + "(" + atributos.get(i).getColumnafk() + ")";

                }
                if (i != atributos.size() - 1) {
                    create += ",\n";
                    query = "";
                } else if (!end.equals("")) {
                    create += ",\n";
                    create += end;
                } else {
                    create += "\n";
                }

                query = "";
                tampered = false;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        create += ")";

        try {
            postgre.createStatement().execute(create);
            return true;
        } catch (Exception e) {

            return false;
        }
    }

    //replicar datos (server -> postgre)
    private boolean replicarDatos1(String schema, String nombre, ArrayList<Atribute> atributos, Date fecha) {
        ResultSet rs;
        try {

            String formato = "YYYY-MM-dd HH:mm:ss:SS";
            SimpleDateFormat format = new SimpleDateFormat(formato);
            String tableName = "cdc." + schema + "_" + nombre + "_CT";
            if (fecha == null) {
                String query = "select * from " + tableName;

                rs = sqlserver.createStatement().executeQuery(query);
            } else {

                String fecha1 = format.format(fecha);

                String query = "select * from cdc.lsn_time_mapping t join cdc.dbo_tablaPrueba2_CT c on t.start_lsn = c.__$start_lsn where tran_begin_time > '" + fecha1 + "'";
                rs = sqlserver.createStatement().executeQuery(query);

            }
            while (rs.next()) {
                String sqlCode = "";
                String operation = "";
                //operaciones
                int op = rs.getInt("__$operation");
                if (op != 3) {
                    switch (op) {
                        case 1:
                            //delete
                            operation = "DELETE";
                            sqlCode = "Delete from " + schema + "." + nombre + " where ";
                            for (Atribute atributo : atributos) {
                                if (atributo.isPrimaryKey()) {
                                    sqlCode += atributo.getName() + "=" + rs.getString(atributo.getName());
                                }
                            }
                            sqlCode += ";";

                            break;
                        case 2:
                            operation = "INSERT";
                            sqlCode = "Insert into " + schema + "." + nombre + " values (";
                            for (int i = 0; i < atributos.size(); i++) {
                                sqlCode += rs.getString(atributos.get(i).getName());
                                if (i != atributos.size() - 1) {
                                    sqlCode += ", ";
                                }
                            }
                            sqlCode += ");";
                            break;
                        case 4:
                            operation = "UPDATE";
                            sqlCode = "Update " + schema + "." + nombre + " set ";
                            for (int i = 0; i < atributos.size(); i++) {
                                sqlCode += atributos.get(i).getName() + " = ";
                                sqlCode += rs.getString(atributos.get(i).getName());
                                if (i != atributos.size() - 1) {
                                    sqlCode += ", ";
                                }
                            }
                            for (Atribute atributo : atributos) {
                                if (atributo.isPrimaryKey()) {
                                    sqlCode += " where " + atributo.getName() + "=" + rs.getString(atributo.getName());
                                }
                            }
                    }
                    System.out.println();
                    String query = String.format("Insert into replicationLog(operation,schema_object,table_object,sql_query,op_date) values ('%s', '%s', '%s', '%s', now());", operation, schema, nombre, sqlCode);
                    postgre.createStatement().execute(query);
                }
            }
            postgre.createStatement().execute("insert into replicationLog(operation,op_date) values ('end', now());");
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
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
