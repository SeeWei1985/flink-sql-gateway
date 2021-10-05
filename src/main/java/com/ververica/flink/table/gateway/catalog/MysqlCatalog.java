package com.ververica.flink.table.gateway.catalog;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.catalog.exceptions.*;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.types.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;

public class MysqlCatalog extends AbstractCatalog {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    public static final String MYSQL_CLASS = "com.mysql.jdbc.Driver";

    private Connection connection;

    public MysqlCatalog(String catalogName, String defaultDatabase, String username, String pwd, String connectUrl) {
        super(catalogName, defaultDatabase);
        try {
            Class.forName(MYSQL_CLASS);
            this.connection = DriverManager.getConnection(connectUrl, username, pwd);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void open() throws CatalogException {
    }

    @Override
    public void close() throws CatalogException {

    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        List<String> res = new ArrayList<>();

        String sql = "select db_name from biz_meta_db";
        try {
            PreparedStatement ps = connection.prepareStatement(sql);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                String db_name = rs.getString("db_name");
                res.add(db_name);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return res;
    }

    @Override
    public CatalogDatabase getDatabase(String databaseName) throws DatabaseNotExistException, CatalogException {
        return null;
    }

    @Override
    public List<String> listTables(String databaseName) throws DatabaseNotExistException, CatalogException {
        List<String> res = new ArrayList<>();
        String sql = "SELECT\n" +
                "\t tb.tb_name as tb_name\n" +
                "FROM\n" +
                "\tbiz_meta_db db\n" +
                "\tjoin biz_meta_db_tb_rel rl on db.id=rl.db_id\n" +
                "\tJOIN biz_meta_table tb ON rl.tb_id = tb.id \n" +
                "WHERE db.db_name =? ";
        try {
            PreparedStatement ps = connection.prepareStatement(sql);
            ps.setString(1, databaseName);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                String tb_name = rs.getString("tb_name");
                res.add(tb_name);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return res;
    }

    @Override
    public List<String> listViews(String databaseName) throws DatabaseNotExistException, CatalogException {
        return null;
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException, CatalogException {

        TableSchema.Builder builder = TableSchema.builder();
        String databaseName = tablePath.getDatabaseName();
        String tableName = tablePath.getObjectName();
        String pk = "";
        String watermarkRowTimeAttribute = "";
        String watermarkExpression = "";
        try {
            PreparedStatement ps = connection.prepareStatement("SELECT\n" +
                    "\ttb.id,\n" +
                    "\tdb.db_name,\n" +
                    "\ttb.tb_name,\n" +
                    "\ttb.primary_key,\n" +
                    "\ttb.watermark_row_time_attribute,\n" +
                    "\ttb.watermark_expression,\n" +
                    "\tcl.field_name,\n" +
                    "\tcl.field_type,\n" +
                    "\tcl.field_size,\n" +
                    "\tcl.expr\n" +
                    "FROM\n" +
                    "\tbiz_meta_db db\n" +
                    "\tjoin biz_meta_db_tb_rel rl on db.id=rl.db_id\n" +
                    "\tJOIN biz_meta_table tb ON rl.tb_id = tb.id\n" +
                    "\tJOIN biz_meta_field cl ON tb.id = cl.tb_id \n" +
                    "WHERE db.db_name =?  \tAND tb.tb_name =? and  cl.partition_flag=0");

            ps.setString(1, databaseName);
            ps.setString(2, tableName);
            ResultSet rs = ps.executeQuery();

            while (rs.next()) {
                if (rs.isFirst()) {
                    pk = rs.getString("primary_key");
                    watermarkRowTimeAttribute = rs.getString("watermark_row_time_attribute");
                    watermarkExpression = rs.getString("watermark_expression");
                }
                String columnName = rs.getString("field_name");
                String columnType = rs.getString("field_type");
                String columnSize = rs.getString("field_size");
                String expr = rs.getString("expr");
                if (expr == null || "".equals(expr)) {
                    builder.field(columnName, mappingType(columnType, columnSize));
                } else {
                    builder.field(columnName, mappingType(columnType, columnSize), expr);
                }
            }

        } catch (Exception e) {
            logger.error("get table fail", e);
        }

        //设置主键
        if (pk != null && !"".equals(pk)) {
            builder.primaryKey(pk.split(","));
        }

        //设置watermark
        if (watermarkExpression != null && !"".equals(watermarkExpression)) {
            builder.watermark(watermarkRowTimeAttribute, watermarkExpression, DataTypes.TIMESTAMP(3));
        }

        TableSchema schema = builder.build();


        List<String> pList = new ArrayList<>();

        String sql = "select a.* from (\n" +
                "SELECT\n" +
                "\tcl.field_name,cl.row_order\n" +
                "FROM\n" +
                "\tbiz_meta_db db\n" +
                "\tjoin biz_meta_db_tb_rel rl on db.id=rl.db_id\n" +
                "\tJOIN biz_meta_table tb ON rl.tb_id = tb.id\n" +
                "\tJOIN biz_meta_field cl ON tb.id = cl.tb_id \n" +
                "WHERE db.db_name =?  \tAND tb.tb_name =?  and cl.partition_flag=1 ) a order by a.row_order desc";


        try {
            PreparedStatement ps = connection.prepareStatement(sql);
            ps.setString(1, databaseName);
            ps.setString(2, tableName);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                String columnName = rs.getString("field_name");
                pList.add(columnName);
            }
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed to list partitions of table %s", tablePath), e);

        }

        return new CatalogTableImpl(schema, pList, getPropertiesFromMysql(databaseName, tableName), "").copy();

    }

    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
        return false;
    }

    @Override
    public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists) throws TableNotExistException, CatalogException {

    }

    @Override
    public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists) throws TableNotExistException, TableAlreadyExistException, CatalogException {

    }

    @Override
    public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists) throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {

    }

    @Override
    public void alterTable(ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists) throws TableNotExistException, CatalogException {

    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath) throws TableNotExistException, TableNotPartitionedException, CatalogException {
        return null;

    }


    private static CatalogPartitionSpec createPartitionSpec(String hivePartitionName) {
//        String[] partKeyVals = hivePartitionName.split("/");
//        Map<String, String> spec = new HashMap<>(partKeyVals.length);
//        for (String keyVal : partKeyVals) {
//            String[] kv = keyVal.split("=");
//            spec.put(unescapePathName(kv[0]), unescapePathName(kv[1]));
//        }
//        return new CatalogPartitionSpec(spec);
        return null;
    }


    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws TableNotExistException, TableNotPartitionedException, CatalogException {
        return null;
    }

    @Override
    public List<CatalogPartitionSpec> listPartitionsByFilter(ObjectPath var1, List<Expression> var2) throws TableNotExistException, TableNotPartitionedException, org.apache.flink.table.catalog.exceptions.CatalogException {
        return null;
    }

    @Override
    public CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws PartitionNotExistException, CatalogException {
        return null;
    }

    @Override
    public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws CatalogException {
        return false;
    }

    @Override
    public void createPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogPartition partition, boolean ignoreIfExists) throws TableNotExistException, TableNotPartitionedException, PartitionSpecInvalidException, PartitionAlreadyExistsException, CatalogException {

    }

    @Override
    public void dropPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, boolean ignoreIfNotExists) throws PartitionNotExistException, CatalogException {

    }

    @Override
    public void alterPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogPartition newPartition, boolean ignoreIfNotExists) throws PartitionNotExistException, CatalogException {

    }

    @Override
    public List<String> listFunctions(String dbName) throws DatabaseNotExistException, CatalogException {
        return Collections.emptyList();
    }

    @Override
    public CatalogFunction getFunction(ObjectPath functionPath) throws FunctionNotExistException, CatalogException {
        throw new FunctionNotExistException(getName(), functionPath);
    }

    @Override
    public boolean functionExists(ObjectPath functionPath) throws CatalogException {
        return false;
    }

    @Override
    public void createFunction(ObjectPath functionPath, CatalogFunction function, boolean ignoreIfExists) throws FunctionAlreadyExistException, DatabaseNotExistException, CatalogException {

    }

    @Override
    public void alterFunction(ObjectPath functionPath, CatalogFunction newFunction, boolean ignoreIfNotExists) throws FunctionNotExistException, CatalogException {

    }

    @Override
    public void dropFunction(ObjectPath functionPath, boolean ignoreIfNotExists) throws FunctionNotExistException, CatalogException {

    }

    @Override
    public CatalogTableStatistics getTableStatistics(ObjectPath tablePath) throws TableNotExistException, CatalogException {
        return null;
    }

    @Override
    public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath) throws TableNotExistException, CatalogException {
        return null;
    }

    @Override
    public CatalogTableStatistics getPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws PartitionNotExistException, CatalogException {
        return null;
    }

    @Override
    public CatalogColumnStatistics getPartitionColumnStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws PartitionNotExistException, CatalogException {
        return null;
    }

    @Override
    public void alterTableStatistics(ObjectPath tablePath, CatalogTableStatistics tableStatistics, boolean ignoreIfNotExists) throws TableNotExistException, CatalogException {

    }

    @Override
    public void alterTableColumnStatistics(ObjectPath tablePath, CatalogColumnStatistics columnStatistics, boolean ignoreIfNotExists) throws TableNotExistException, CatalogException, TablePartitionedException {

    }

    @Override
    public void alterPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogTableStatistics partitionStatistics, boolean ignoreIfNotExists) throws PartitionNotExistException, CatalogException {

    }

    @Override
    public void alterPartitionColumnStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogColumnStatistics columnStatistics, boolean ignoreIfNotExists) throws PartitionNotExistException, CatalogException {

    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {

        String sql = "select count(*) from biz_meta_db where db_name = ?";
        try {
            PreparedStatement ps = connection.prepareStatement(sql);
            ps.setString(1, databaseName);
            ResultSet rs = ps.executeQuery();

            while (rs.next()) {
                int count = rs.getInt(1);
                if (count == 0) {
                    return false;
                } else {
                    return true;
                }
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;

    }

    @Override
    public void createDatabase(String name, CatalogDatabase database, boolean ignoreIfExists) throws DatabaseAlreadyExistException, CatalogException {

    }

    @Override
    public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade) throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {

    }

    @Override
    public void alterDatabase(String name, CatalogDatabase newDatabase, boolean ignoreIfNotExists) throws DatabaseNotExistException, CatalogException {

    }

    private Map<String, String> getPropertiesFromMysql(String databaseName, String tableName) {
        Map<String, String> map = new HashMap<>();

        String sql = "SELECT\n" +
                "\tfl.field_key as `key`,\n" +
                "  fl.choose_val as `value`\n" +
                "\n" +
                "FROM\n" +
                "\tbiz_meta_db db\n" +
                "\tjoin biz_meta_db_tb_rel rl on db.id=rl.db_id\n" +
                "\tJOIN biz_meta_table tb ON rl.tb_id = tb.id\n" +
                "\tJOIN biz_meta_table_field_rel fl ON tb.id = fl.tb_id \n" +
                "WHERE db.db_name =?  \tAND tb.tb_name =?";
        try {
            PreparedStatement ps = connection.prepareStatement(sql);
            ps.setString(1, databaseName);
            ps.setString(2, tableName);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                map.put(rs.getString("key"), rs.getString("value"));
            }
        } catch (Exception e) {
            logger.error("get table's properties fail", e);
        }

        return map;
    }

    /**
     * 根据数据库中存储的数据类型，返回flink中的数据类型
     *
     * @param mysqlType
     * @return
     */
    private DataType mappingType(String mysqlType, String columnSize) {
        mysqlType = mysqlType.toLowerCase();

        switch (mysqlType) {
            case "bigint":
                return DataTypes.BIGINT();
            case "double":
                return DataTypes.DOUBLE();
            case "string":
                return DataTypes.STRING();
            case "decimal": {
                String[] tmp = columnSize.split(",");
                return DataTypes.DECIMAL(Integer.parseInt(tmp[0]), Integer.parseInt(tmp[1]));
            }
            case "char": {
                return DataTypes.CHAR(Integer.parseInt(columnSize));
            }
            case "varchar": {
                return DataTypes.VARCHAR(Integer.parseInt(columnSize));
            }
            case "boolean": {
                return DataTypes.BOOLEAN();
            }
            case "timestamp":
                return DataTypes.TIMESTAMP(3);
            default:
                throw new UnsupportedOperationException("current not support " + mysqlType);
        }
    }


    /**
     * 由FactoryUtils调用，如果返回空，就根据connector字段来判断，利用Java SPI去实现工厂的获取
     * AbstractJdbcCatalog默认会返回Jdbc动态工厂这是不对的
     *
     * @return
     */
    @Override
    public Optional<Factory> getFactory() {
        return Optional.empty();
    }
}
