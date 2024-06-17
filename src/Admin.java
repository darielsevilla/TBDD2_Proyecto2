
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
    private ArrayList<String> previousReplicated;
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
        previousReplicated = new ArrayList<>();
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
            postgre = DriverManager.getConnection("jdbc:" + instancia_postgre + ":" + puerto_postgre + "/" + name_postgre, user_postgre, pass_postgre);
            sqlserver = DriverManager.getConnection("jdbc:" + instancia_sql + ":" + puerto_sqlserver + ";database=" + name_sqlserver + ";user=" + user_sqlserver + ";password=" + pass_sqlserver + ";encrypt=true;" + "trustServerCertificate=true;" + "loginTimeout=30;");
            loadTablas();

            return true;
        } catch (Exception e) {

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
                    if (!results.getString("table_name").equals("replicationlog")) {
                        tablasSinReplicar.add(results.getString("table_schema") + "." + results.getString("table_name"));
                    }

                }
            } catch (Exception e) {

            }
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
                if (tipo.contains("char")) {
                    size /= 2;
                }
                //revisa si es foreign key
                String llaveForanea = "SELECT f.name AS foreign_key_name,\n"
                        + "                            OBJECT_NAME(f.parent_object_id) AS table_name,\n"
                        + "							OBJECT_SCHEMA_NAME(f.parent_object_id) as schemaName,\n"
                        + "                            COL_NAME(fc.parent_object_id, fc.parent_column_id) AS constraint_column_name,\n"
                        + "							OBJECT_SCHEMA_NAME(f.referenced_object_id) as schemaReference,\n"
                        + "                            OBJECT_NAME(f.referenced_object_id) AS referenced_object,\n"
                        + "                            COL_NAME(fc.referenced_object_id, fc.referenced_column_id) AS referenced_column_name\n"
                        + "                        FROM sys.foreign_keys AS f\n"
                        + "                        INNER JOIN sys.foreign_key_columns AS fc\n"
                        + "                            ON f.object_id = fc.constraint_object_id\n"
                        + "                        WHERE f.parent_object_id = OBJECT_ID('" + table + "') AND COL_NAME(fc.parent_object_id, fc.parent_column_id) = '" + nombre + "';\n"
                        + "				";

                ResultSet foranea = sqlserver.createStatement().executeQuery(llaveForanea);
                String rCol = null;
                while (foranea.next()) {

                    isFK = true;
                    String schemaa = foranea.getString("schemaReference");
                    reference = foranea.getString("referenced_object");

                    rCol = foranea.getString("referenced_column_name");
                    boolean isThere = false;
                    for (String str : this.tablasReplicadas) {
                        String s_name = "dbo";
                        String t_name = str;
                        String[] array = str.replace('.', '-').split("-");

                        //separar el nombre del esquema 
                        if (array.length > 1) {
                            //verificar si el esquema existe

                            s_name = array[0];
                            t_name = array[1];
                        }

                        if (s_name.equals(schemaa) && t_name.equals(reference)) {
                            isThere = true;

                            break;
                        }

                    }
                    reference = schemaa + "." + reference;
                    /*if (!isThere) {

                        return null;
                    }*/
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
                    reference = fk.getString("foreign_table_schema") + "." + fk.getString("foreign_table_name");
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

    public ArrayList<String> getPreviousReplicated() {
        return previousReplicated;
    }

    public void setPreviousReplicated(ArrayList<String> previousReplicated) {
        this.previousReplicated.clear();
        for (String string : previousReplicated) {
          this.previousReplicated.add(string);  
        }
 
    }
    
    public boolean replicarPostgreToServer(Date date) {
        try {

            ArrayList<Atribute> atributos;
            if (!reorderListPostgres()) {

                return false;
            }
            ResultSet existe;
            for (int i = 0; i < tablasReplicadas.size(); i++) {
                //verificar si la tabla existe

                atributos = getAtributos_postgre(tablasReplicadas.get(i));
                if (atributos == null) {
                    return false;
                }
                String schema = "dbo";
                String name = "";

                String[] array = tablasReplicadas.get(i).replace('.', '-').split("-");

                //separar el nombre del esquema 
                if (array.length > 1) {
                    //verificar si el esquema existe
                    if (!array[0].equals("public")) {
                        schema = array[0];
                    }
                    name = array[1];
                    String schemaqueue = "select schema_name from information_schema.schemata where schema_name = '" + schema + "';";
                    existe = sqlserver.createStatement().executeQuery(schemaqueue);
                    if (!existe.next()) {
                        //si no existe se crea
                        sqlserver.createStatement().execute("create schema " + schema);
                    }
                } else {
                    name = array[0];
                }

                //ahora verifica si la tabla existe
                existe = sqlserver.createStatement().executeQuery("select table_name from information_schema.tables where table_name = '" + name + "' and table_schema = '" + schema + "';");
                //si la tabla no existe la crea

                /*if (!existe.next()) {
                    boolean replicate = replicarEstructura2(tablasReplicadas.get(i), atributos);

                }*/
                //borrar----
                switch (this.checkTables(tablasReplicadas.get(i), this.getAtributos_postgre(tablasReplicadas.get(i)), 2)) {
                    case -2:

                        String sql = "drop table " + tablasReplicadas.get(i).replace("public.", "dbo.") + ";";
                        sqlserver.createStatement().execute(sql);
                        replicarEstructura2(tablasReplicadas.get(i), atributos);

                        System.out.println("existe");
                        break;
                    case -1:

                        System.out.println("no existe");
                        replicarEstructura2(tablasReplicadas.get(i), atributos);
                        break;
                }
                //borrar----
                //replica datos
                if (!replicarDatos2(schema, name, date)) {
                    return false;
                }

            }
            String endEntry = "INSERT INTO replicationLog(op_id, operation)\n"
                    + "SELECT COUNT(*) + 1 AS c, 'end' AS op\n"
                    + "FROM replicationLog;";
            sqlserver.createStatement().execute(endEntry);

        } catch (SQLException ex) {

            return false;
        }
        return true;

    }

    public boolean replicarEstructura2(String nombre, ArrayList<Atribute> atributos) {

        String[] split = nombre.replace('.', '-').split("-");
        if (split.length == 2) {
            if (split[0].equals("public")) {

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
                } else if (name.equals("bpchar")) {
                    name = "char(1)";
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
                    if (!end.equals("")) {
                        end += ",";
                    }
                    end += " foreign key (" + atributos.get(i).getName() + ") references " + atributos.get(i).getReference().replace("public.", "dbo.") + "(" + atributos.get(i).getColumnafk() + ")";

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

        }
        create += ");";

        try {

            sqlserver.createStatement().execute(create);
            return true;
        } catch (Exception e) {

            return false;
        }
    }

    public boolean reorderListServer() {
        try {
            boolean done = false;
            do {
                done = true;
                for (int i = 0; i < this.tablasReplicadas.size(); i++) {
                    int currentPos = i;
                    String llaveForanea = "SELECT f.name AS foreign_key_name,\n"
                            + "                            OBJECT_NAME(f.parent_object_id) AS table_name,\n"
                            + "							OBJECT_SCHEMA_NAME(f.parent_object_id) as schemaName,\n"
                            + "                            COL_NAME(fc.parent_object_id, fc.parent_column_id) AS constraint_column_name,\n"
                            + "							OBJECT_SCHEMA_NAME(f.referenced_object_id) as schemaReference,\n"
                            + "                            OBJECT_NAME(f.referenced_object_id) AS referenced_object,\n"
                            + "                            COL_NAME(fc.referenced_object_id, fc.referenced_column_id) AS referenced_column_name\n"
                            + "                        FROM sys.foreign_keys AS f\n"
                            + "                        INNER JOIN sys.foreign_key_columns AS fc\n"
                            + "                            ON f.object_id = fc.constraint_object_id\n"
                            + "                        WHERE f.parent_object_id = OBJECT_ID('" + tablasReplicadas.get(i) + "');";
                    ResultSet result = sqlserver.createStatement().executeQuery(llaveForanea);
                    while (result.next()) {

                        String nameReference = result.getString("schemaReference") + "." + result.getString("referenced_object");
                        boolean foreign = false;
                        for (int j = 0; j < this.tablasReplicadas.size(); j++) {
                            String actual = this.tablasReplicadas.get(j);
                            if (actual.equals(nameReference)) {
                                foreign = true;

                                if (j > currentPos) {
                                    this.tablasReplicadas.add(j + 1, this.tablasReplicadas.get(currentPos));
                                    this.tablasReplicadas.remove(currentPos);
                                    done = false;
                                    if (currentPos == i) {
                                        i--;
                                    }
                                    currentPos = j;
                                }

                                break;
                            }
                        }
                        if (!foreign) {
                            return false;
                        }
                    }
                }

            } while (!done);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }

    }

    public boolean backwardsOrderServer() {
        try {
            boolean done = false;
            do {
                done = true;
                for (int i = 0; i < this.previousReplicated.size(); i++) {
                    int currentPos = i;
                    String llaveForanea = "SELECT f.name AS foreign_key_name,\n"
                            + "                            OBJECT_NAME(f.parent_object_id) AS table_name,\n"
                            + "							OBJECT_SCHEMA_NAME(f.parent_object_id) as schemaName,\n"
                            + "                            COL_NAME(fc.parent_object_id, fc.parent_column_id) AS constraint_column_name,\n"
                            + "							OBJECT_SCHEMA_NAME(f.referenced_object_id) as schemaReference,\n"
                            + "                            OBJECT_NAME(f.referenced_object_id) AS referenced_object,\n"
                            + "                            COL_NAME(fc.referenced_object_id, fc.referenced_column_id) AS referenced_column_name\n"
                            + "                        FROM sys.foreign_keys AS f\n"
                            + "                        INNER JOIN sys.foreign_key_columns AS fc\n"
                            + "                            ON f.object_id = fc.constraint_object_id\n"
                            + "                        WHERE f.parent_object_id = OBJECT_ID('" + this.previousReplicated.get(i) + "');";
                    ResultSet result = sqlserver.createStatement().executeQuery(llaveForanea);
                    while (result.next()) {

                        String nameReference = result.getString("schemaReference") + "." + result.getString("referenced_object");
                        boolean foreign = false;
                        for (int j = 0; j < this.previousReplicated.size(); j++) {
                            String actual = this.previousReplicated.get(j);
                            if (actual.equals(nameReference)) {
                                foreign = true;

                                if (j < currentPos) {                                    
                                    this.previousReplicated.add(j, this.previousReplicated.get(currentPos));
                                    this.previousReplicated.remove(currentPos+1);
                                    done = false;
                                    if (currentPos == i) {
                                        i--;
                                    }
                                    currentPos = j;
                                }

                                break;
                            }
                        }
                        if (!foreign) {
                            return false;
                        }
                    }
                }

            } while (!done);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }

    }

    public boolean reorderListPostgres() {
        try {
            boolean done = false;
            do {
                done = true;
                for (int i = 0; i < this.tablasReplicadas.size(); i++) {
                    int currentPos = i;
                    String schema = "public";
                    String tabla = this.tablasReplicadas.get(i);
                    String[] array = tablasReplicadas.get(i).replace('.', '-').split("-");

                    //separar el nombre del esquema 
                    if (array.length > 1) {
                        //verificar si el esquema existe
                        schema = array[0];
                        tabla = array[1];

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
                            + "    AND tc.table_name='" + tabla + "'\n";

                    ResultSet result = postgre.createStatement().executeQuery(sql);
                    while (result.next()) {

                        String nameReference = result.getString("foreign_table_schema") + "." + result.getString("foreign_table_name");

                        boolean foreign = false;
                        for (int j = 0; j < this.tablasReplicadas.size(); j++) {

                            String actual = this.tablasReplicadas.get(j);

                            if (actual.equals(nameReference)) {
                                foreign = true;

                                if (j > currentPos) {
                                    this.tablasReplicadas.add(j + 1, this.tablasReplicadas.get(currentPos));
                                    this.tablasReplicadas.remove(currentPos);
                                    done = false;
                                    if (currentPos == i) {
                                        i--;
                                    }
                                    currentPos = j;
                                }

                                break;
                            }
                        }
                        if (!foreign) {
                            return false;
                        }
                    }
                }

            } while (!done);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }

    }

    public boolean backwardsOrderPostgres() {
        try {
            boolean done = false;
            do {
                done = true;
                for (int i = 0; i < this.previousReplicated.size(); i++) {
                    int currentPos = i;
                    String schema = "public";
                    String tabla = this.previousReplicated.get(i);
                    String[] array = this.previousReplicated.get(i).replace('.', '-').split("-");

                    //separar el nombre del esquema 
                    if (array.length > 1) {
                        //verificar si el esquema existe
                        schema = array[0];
                        tabla = array[1];

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
                            + "    AND tc.table_name='" + tabla + "'\n";

                    ResultSet result = postgre.createStatement().executeQuery(sql);
                    while (result.next()) {

                        String nameReference = result.getString("foreign_table_schema") + "." + result.getString("foreign_table_name");

                        boolean foreign = false;
                        for (int j = 0; j < this.previousReplicated.size(); j++) {

                            String actual = this.previousReplicated.get(j);

                            if (actual.equals(nameReference)) {
                                foreign = true;

                                if (j < currentPos) {
                                    this.previousReplicated.add(j, this.previousReplicated.get(currentPos));
                                    this.previousReplicated.remove(currentPos+1);
                                    done = false;
                                    if (currentPos == i) {
                                        i--;
                                    }
                                    currentPos = j;
                                }

                                break;
                            }
                        }
                        if (!foreign) {
                            return false;
                        }
                    }
                }

            } while (!done);
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
            if (!reorderListServer()) {
           
                return false;
            }
      
            ResultSet existe;
            for (int i = 0; i < tablasReplicadas.size(); i++) {
                //verificar si la tabla existe

                atributos = getAtributos_sqlserver(tablasReplicadas.get(i));
                if (atributos == null) {

                    return false;
                }
                String schema = "public";
                String name = "";

                String[] array = tablasReplicadas.get(i).replace('.', '-').split("-");

                //separar el nombre del esquema 
                if (array.length > 1) {
                    //verificar si el esquema existe
                    if (!array[0].equals("dbo")) {
                        schema = array[0];
                    }
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
                /*
                //ahora verifica si la tabla existe
                existe = postgre.createStatement().executeQuery("select table_name from information_schema.tables where table_name = '" + name + "' and table_schema = '" + schema.replace("public.", "dbo.") + "'");
                //si la tabla no existe la crea

                if (!existe.next()) {
                    boolean replicate = replicarEstructura1(tablasReplicadas.get(i), atributos);

                }*/

                //--borrar aqui
                switch (this.checkTables(tablasReplicadas.get(i).replace("dbo.", "public."), this.getAtributos_postgre(tablasReplicadas.get(i).replace("dbo.", "public.")), 1)) {
                    case -2:
                        String sql = "drop table " + tablasReplicadas.get(i).replace("dbo.", "public.") + ";";

                        replicarEstructura1(tablasReplicadas.get(i), atributos);

                        System.out.println("existe");
                        break;
                    case -1:
                        System.out.println("no existe");
                        replicarEstructura1(tablasReplicadas.get(i), atributos);
                        break;
                }
                //--borrar aqui
                if (schema.equals("public")) {
                    schema = "dbo";
                }
                //replica datos
                if (!replicarDatos1(schema, name, atributos, date)) {
                    return false;
                }

            }
            postgre.createStatement().execute("insert into replicationLog(operation,op_date) values ('end', now());");
        } catch (SQLException ex) {
            return false;
        }
        return true;
    }

    //replicación de la estructura de la tabla en si (server -> postgre)
    private boolean replicarEstructura1(String nombre, ArrayList<Atribute> atributos) {
        String tableName = nombre;

        String[] array = nombre.replace('.', '-').split("-");

        //separar el nombre del esquema 
        if (array.length > 1) {
            //verificar si el esquema existe
            if (array[0].equals("dbo")) {
                tableName = array[1];
            }

        }
        String create = "create table " + tableName + "(";
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

                    existe = postgre.createStatement().executeQuery(queueAlias);
                    //si el alias ya existe es q o es un tipo de dato en postgre o ya fue definido
                    if (existe.next()) {
                        name = alias;
                        alias = null;

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
                    if (!end.equals("")) {
                        end += ",";
                    }

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

    private boolean replicarDatos2(String schema, String nombre, Date fecha) {
        ResultSet rs;
        String schemaP = schema;
        
        if (schemaP.toLowerCase().equals("dbo")) {
            schemaP = "public";
        }
        try {
            String formato = "YYYY-MM-dd HH:mm:ss.SS";
            SimpleDateFormat format = new SimpleDateFormat(formato);
            if (fecha == null) {

                String query = "select * from bitacora where schema_object='" + schemaP + "' and table_object='" + nombre + "';";
                rs = postgre.createStatement().executeQuery(query);
            } else {
                String fecha1 = format.format(fecha);
                String query = "select * from bitacora where schema_object='" + schemaP + "' and table_object='" + nombre + "' and op_date > '" + fecha1 + "'";
                rs = postgre.createStatement().executeQuery(query);

            }

            while (rs.next()) {
                ResultSet num = sqlserver.createStatement().executeQuery("select count(op_id) as c from replicationLog");
                num.next();
                int id = num.getInt("c") + 1;

                String query = String.format("insert into replicationLog(op_id, operation, schema_object, table_object, sql_query) values (%d,'%s','%s','%s','%s')", id, rs.getString("operation"), schema, rs.getString("table_object"), rs.getString("sql_query").replace("'", "''").replace("public."+rs.getString("table_object"), "dbo."+rs.getString("table_object")));
                
                sqlserver.createStatement().execute(query);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return true;
    }

    //replicar datos (server -> postgre)
    private boolean replicarDatos1(String schema, String nombre, ArrayList<Atribute> atributos, Date fecha) {
        ResultSet rs;
        int cont = 0;
        try {

            String formato = "YYYY-MM-dd HH:mm:ss:SS";
            SimpleDateFormat format = new SimpleDateFormat(formato);
            String tableName = "cdc." + schema + "_" + nombre + "_CT";
            if (fecha == null) {
                String query = "select * from " + tableName;

                rs = sqlserver.createStatement().executeQuery(query);
            } else {

                String fecha1 = format.format(fecha);

                String query = "select * from cdc.lsn_time_mapping t join " + tableName + " c on t.start_lsn = c.__$start_lsn where tran_begin_time > '" + fecha1 + "'";
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

                                    sqlCode += atributo.getName() + " = ";
                                    if (atributo.getDataType().contains("char")) {
                                        sqlCode += "''" + rs.getString(atributo.getName()) + "''";
                                    } else {
                                        sqlCode += rs.getString(atributo.getName());
                                    }
                                }
                            }
                            sqlCode += ";";

                            break;
                        case 2:
                            operation = "INSERT";
                            sqlCode = "Insert into " + schema + "." + nombre + " values (";
                            for (int i = 0; i < atributos.size(); i++) {
                                if (atributos.get(i).getDataType().contains("char")) {
                                    sqlCode += "''" + rs.getString(atributos.get(i).getName()) + "''";
                                } else {
                                    sqlCode += rs.getString(atributos.get(i).getName());
                                }

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
                                if (atributos.get(i).getDataType().contains("char")) {
                                    sqlCode += "''" + rs.getString(atributos.get(i).getName()) + "''";
                                } else {
                                    sqlCode += rs.getString(atributos.get(i).getName());
                                }
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

                    String query = String.format("Insert into replicationLog(operation,schema_object,table_object,sql_query,op_date) values ('%s', '%s', '%s', '%s', now());", operation, schema, nombre, sqlCode);
                    cont++;
                    postgre.createStatement().execute(query);
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public boolean borrarNoReplicados() {
        try {

            for(int i = 0; i < this.previousReplicated.size(); i++){
                if(tablasReplicadas.contains(this.previousReplicated.get(i))){
                    previousReplicated.remove(i);
                    i--;
                }
            }
            if (cual == 1) {
                if(!backwardsOrderServer()){
                    return false;
                }
            } else if (cual == 2) {
                if(!backwardsOrderPostgres()){
                    return false;
                }
            }
            this.printPrevious();
            System.out.println("previously");
            for (String tabla : this.previousReplicated) {
                System.out.println(tabla);
                if (cual == 1) {
                    postgre.createStatement().execute("drop table " + tabla.replace("dbo.", "public.") + ";");
                } else {
                    sqlserver.createStatement().execute("drop table " + tabla.replace("public.", "dbo.") + ";");
                    
                }
            }
            return true;
        } catch (Exception e) {
        e.printStackTrace();
            return false;
        }
    }

    public void printSinReplicar() {
        for (int i = 0; i < tablasSinReplicar.size(); i++) {
            System.out.println(tablasSinReplicar.get(i));
        }
    }

    public void printReplicar() {
        for (int i = 0; i < tablasReplicadas.size(); i++) {
            System.out.println(tablasReplicadas.get(i));
        }
    }
    
     public void printPrevious() {
        for (int i = 0; i < this.previousReplicated.size(); i++) {
            System.out.println(this.previousReplicated.get(i));
        }
    }


    public int checkTables(String tabla, ArrayList<Atribute> atributos_postgres, int cual) {
        int retorno = -1;
        String schema = "dbo";
        String nombre = tabla;
        String[] array = tabla.replace('.', '-').split("-");
        if (array.length > 1) {
            if (!array[0].equals("public")) {
                schema = array[0];
            }
            nombre = array[1];
        }

        try {
            ResultSet existe = sqlserver.createStatement().executeQuery("select table_name from information_schema.tables where table_name = '" + nombre + "' and table_schema = '" + schema + "';");

            while (existe.next()) {
                retorno = 1;

                ArrayList<Atribute> atributos_sql = this.getAtributos_sqlserver((schema + "." + nombre));
                if (atributos_sql.size() != atributos_postgres.size()) {
                    return -2;
                }
                /*System.out.println(tabla);
                System.out.println("-----------------------");
                this.printAtributos(atributos_postgres);

                System.out.println("\n" + schema + "." + nombre);
                System.out.println("------------------------");
                this.printAtributos(atributos_sql);*/
                for (Atribute atributepos : atributos_postgres) {
                    boolean isThere = false;

                    //cambiar atributos postgres
                    if (atributepos.getDataType().equals("int4")) {
                        atributepos.setDataType("int");

                    } else if (atributepos.getDataType().equals("float8")) {
                        atributepos.setDataType("float");

                    } else if (atributepos.getDataType().equals("bpchar")) {
                        atributepos.setDataType("char(1)");

                    } else if (atributepos.getDataType().equals("serial")) {
                        atributepos.setDataType("int");
                    }

                    for (Atribute atributesql : atributos_sql) {
                        String name = atributesql.getDataType();
                        //cambiar atributos sqlserver
                        if (name.equals("tinyint")) {
                            name = "smallint";
                        } else if (name.equals("smallmoney")) {
                            name = "money";
                        } else if (name.equals("smalldatetime")) {
                            name = "timestamp(0)";
                        } else if (name.equals("datetime")) {
                            name = "timestamp(3)";
                        } else if (name.equals("datetime2")) {
                            name = "timestamp(3)";
                        } else if (name.equals("datetimeoffset")) {
                            name = "timestamp(3) with time zone";
                        } else if (name.equals("binary") || name.equals("varbinary") || name.equals("image") || name.equals("varbinary(max)")) {
                            name = "bytea";
                        } else if (name.equals("ntext") || name.equals("nvarchar(max)") || name.equals("varchar(max)")) {
                            name = "text";
                        } else if (name.equals("uniqueidentifier")) {
                            name = "char(16)";
                        } else if (name.equals("hierarchyid")) {
                            name = "varchar(4000)";
                        } else if (name.equals("rowversion")) {
                            name = "timestamp(3)";
                        } else if (name.equals("nvarchar")) {
                            name = "varchar";
                        }
                        atributesql.setDataType(name);

                        int equals = 1;
                        if (atributepos.getName().equals(atributesql.getName()) && atributepos.isPrimaryKey() == atributesql.isPrimaryKey() && atributepos.isForeignKey() == atributesql.isForeignKey()) {
                            //revision de foreign keys
                            if (atributepos.isForeignKey()) {
                                equals--;
                                if (atributepos.getReference().equals(atributesql.getReference().replace("dbo.", "public.")) && atributepos.getColumnafk().equals(atributesql.getColumnafk())) {
                                    equals++;
                                }
                            }
                            if (atributepos.getDataType().contains("char")) {
                                String dataP = atributepos.getDataType();
                                String dataS = atributesql.getDataType();
                                //revision de tamaños
                                int sizep = atributepos.getSize() / 2;
                                int sizes = atributesql.getSize();

                                if (!atributesql.getDataType().replaceAll("[^0-9]", "").equals("")) {
                                    sizes = Integer.parseInt(atributesql.getDataType().replaceAll("[^0-9]", ""));
                                }
                                if (!atributepos.getDataType().replaceAll("[^0-9]", "").equals("")) {
                                    sizep = Integer.parseInt(atributepos.getDataType().replaceAll("[^0-9]", ""));
                                }
                                dataP = dataP.replace("(", "").replace(")", "").replaceAll("[0-9]", "");
                                dataS = dataS.replace("(", "").replace(")", "").replaceAll("[0-9]", "");
                                if (dataP.equals(dataS)) {
                                    if (sizes >= sizep && cual == 2) {
                                        equals++;
                                    } else if (sizep >= sizes && cual == 1) {
                                        equals++;
                                    }

                                }
                            } else {
                                equals++;
                            }
                            if (equals == 2) {

                                isThere = true;
                                break;
                            }
                        }
                    }
                    //System.out.println(atributepos.getName());
                    if (!isThere) {

                        return -2;
                    }
                }

            }
            if (retorno == -1) {

            }
            return retorno;
        } catch (Exception e) {
            e.printStackTrace();
            return -1;
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
