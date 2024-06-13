/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */

/**
 *
 * @author darie
 */
public class Atribute {
    private String name;
    private boolean foreignKey;
    private boolean primaryKey;
    private boolean isNull;
    private String dataType;
    private String reference;
    private int size;
    private String alias;
    private int precision;
    private String columnafk;
    public Atribute(String name, boolean foreignKey, boolean primaryKey, String dataType, String reference, int size, boolean isNull, String alias, int precision, String columnafk) {
        this.name = name;
        this.foreignKey = foreignKey;
        this.primaryKey = primaryKey;
        this.dataType = dataType;
        this.reference = reference;
        this.size = size;
        this.isNull = isNull;
        this.alias = alias;
        this.precision = precision;
        this.columnafk = columnafk;
    }

    public int getPrecision() {
        return precision;
    }

    public void setPrecision(int precision) {
        this.precision = precision;
    }

    public String getColumnafk() {
        return columnafk;
    }

    public void setColumnafk(String columnafk) {
        this.columnafk = columnafk;
    }

    
    
    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    
    public boolean isIsNull() {
        return isNull;
    }

    public void setIsNull(boolean isNull) {
        this.isNull = isNull;
    }

    
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean isForeignKey() {
        return foreignKey;
    }

    public void setForeignKey(boolean foreignKey) {
        this.foreignKey = foreignKey;
    }

    public boolean isPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(boolean primaryKey) {
        this.primaryKey = primaryKey;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public String getReference() {
        return reference;
    }

    public void setReference(String reference) {
        this.reference = reference;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

 
    @Override
    public String toString() {
        return "Atribute{" + "name=" + name + ", foreignKey=" + foreignKey + ", primaryKey=" + primaryKey + ", isNull=" + isNull + ", dataType=" + dataType + ", reference=" + reference + ", size=" + size + ", alias=" + alias + ", precision=" + precision + ", columnafk=" + columnafk + '}';
    }
   
    
    
    
}
