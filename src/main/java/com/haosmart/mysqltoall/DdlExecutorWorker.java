package com.haosmart.mysqltoall;

import cn.hutool.core.lang.Assert;
import cn.hutool.core.text.CharSequenceUtil;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.PageUtil;
import cn.hutool.core.util.ReUtil;
import cn.hutool.db.*;
import cn.hutool.db.meta.Column;
import cn.hutool.db.meta.MetaUtil;
import cn.hutool.db.meta.Table;
import cn.hutool.log.level.Level;
import com.haosmart.mysqltoall.ddl.DdlProvider;
import com.haosmart.mysqltoall.entity.TableMeta;
import com.haosmart.mysqltoall.enumration.ActionType;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class DdlExecutorWorker {

    private final DataSourceProvider sourceProvider;

    private final DataSourceProvider targetProvider;

    private final DdlProvider ddlProvider;

    private final ExecuteStrategy executeStrategy;

    private final Connection connection;

    private final static String QUERY_WITH_CONDITION = "select * from {} where {}";

    private final static String QUERY_ALL = "select * from {}";

    private final static String QUERY_COUNT = "select count(*) from {}";

    private final static String QUERY_COUNT_WITH_CONDITION = "select count(*) from {}  where {}";

    private final static String DELETE_ALL = "delete from {}";

    public DdlExecutorWorker(DataSourceProvider sourceProvider, DataSourceProvider targetProvider, DdlProvider ddlProvider, ExecuteStrategy executeStrategy, Connection connection) {
        this.sourceProvider = sourceProvider;
        this.targetProvider = targetProvider;
        this.ddlProvider = ddlProvider;
        this.executeStrategy = executeStrategy;
        this.connection = connection;
        if (this.executeStrategy.debugMode()) {
            DbUtil.setShowSqlGlobal(true, true, true, Level.DEBUG);
        }
    }


    /**
     * 同步多张表
     *
     * @param tableList 表名列表
     */
    public void transferTableList(List<TableMeta> tableList) throws Throwable {
        if (ObjectUtil.isNotEmpty(tableList)) {
            int i = 1;
            for (TableMeta tableMeta : tableList) {
                if (!this.shouldIgnore(tableMeta)) {
                    try {
                        this.transferSingleTable(tableMeta, null);
                        log.warn("Table[{}] ({}/{}) transfer succeed", tableMeta.getTableName(), i, tableList.size());
                    } catch (Throwable e) {
                        if (!this.ddlProvider.handleThrowable(e, tableMeta.getTableName())) {
                            log.error("Error in sync data [table:{} error:{}] ", tableMeta.getTableName(), e.getMessage());
                        }
                        if (!this.executeStrategy.continueWhenError()) {
                            throw e;
                        }
                    }
                    i++;
                } else {
                    log.warn("According to the execute strategy, table[{}] has been skipped", tableMeta.getTableName());
                }
            }
        }
    }

    /**
     * 同步单张表
     *
     * @param tableMeta 表对象
     * @param condition 数据过滤条件
     */
    public void transferSingleTable(TableMeta tableMeta, String condition) throws Exception {
        if (ObjectUtil.isNull(tableMeta)) {
            return;
        }
        Table fromTable = this.getTable(this.sourceProvider.getDataSource(), tableMeta.getTableName());
        if (!this.isTableExists(fromTable)) {
            log.error("Table:{} not found", tableMeta.getTableName());
            return;
        }
        Table toTable = this.getTable(this.targetProvider.getDataSource(), tableMeta.getTableName());
        if ((!this.isTableExists(toTable))
                || ObjectUtil.equals(ExecuteStrategy.BuildType.DELETE_AND_REBUILD, this.executeStrategy.getBuildType())) {
            toTable = this.recreateTable(fromTable, toTable);
            if (this.executeStrategy.isIncludeData()) {
                this.insertData(tableMeta, fromTable, toTable, condition);
            }
        } else {
            log.warn("Table:{} already existed", tableMeta.getTableName());
        }
    }


    /**
     * 判断表是否存在
     *
     * @param table 表对象
     * @return 是否存在
     */
    private boolean isTableExists(Table table) {
        return ObjectUtil.isNotNull(table) && ObjectUtil.isNotEmpty(table.getColumns());
    }

    private boolean shouldIgnore(@NonNull TableMeta tableMeta) {
        if (tableMeta.getRecordCount() > -1L && tableMeta.getRecordCount() > this.executeStrategy.getMaxRecordCount()) {
            return true;
        }
        if (ObjectUtil.isNotEmpty(this.executeStrategy.getIgnoredTableNames())) {
            for (String tableNameRegex : this.executeStrategy.getIgnoredTableNames()) {
                if (ReUtil.isMatch(tableNameRegex, tableMeta.getTableName())) {
                    return true;
                }
            }
        }
        return false;
    }


    /**
     * 重新创建表结构及索引
     *
     * @param fromTable 来源表
     * @param toTable   目标表
     */
    private Table recreateTable(Table fromTable, Table toTable) throws Exception {
        Db db = Db.use(this.connection);
        if (this.isTableExists(toTable)) {
            this.deleteTable(db, toTable);
        }
        this.createTable(db, fromTable);
        Table table = this.getTable(this.targetProvider.getDataSource(), fromTable.getTableName());
        Assert.isTrue(this.isTableExists(table), "Recreate Table:{} failed", fromTable.getTableName());
        log.info("CREATE TABLE {}", table.getTableName());
        return table;
    }


    /**
     * 删除表结构及索引
     *
     * @param db      数据库对象
     * @param toTable 目标表对象
     */
    private void deleteTable(Db db, Table toTable) throws Exception {
        List<String> deleteTableSqlList = ddlProvider.buildDdl(this.sourceProvider.getConfig(), this.targetProvider.getConfig(), toTable, ActionType.DELETE);
        if (ObjectUtil.isNotEmpty(deleteTableSqlList)) {
            try {
                log.info("DROP TABLE {}", toTable.getTableName());
                db.executeBatch(deleteTableSqlList);
            } catch (Exception e) {
                //索引不存在
                if (!this.ddlProvider.handleThrowable(e, toTable.getTableName())) {
                    throw e;
                }
            }
        }
    }


    /**
     * 创建表结构及索引
     *
     * @param db        数据库对象
     * @param fromTable 来源表对象
     */
    private void createTable(Db db, Table fromTable) throws Exception {
        List<String> createTableSqlList = ddlProvider.buildDdl(this.sourceProvider.getConfig(), this.targetProvider.getConfig(), fromTable, ActionType.CREATE);
        if (ObjectUtil.isNotEmpty(createTableSqlList)) {
            try {
                db.executeBatch(createTableSqlList);
            } catch (Exception e) {
                //索引已存在
                if (!this.ddlProvider.handleThrowable(e, fromTable.getTableName())) {
                    throw e;
                }
            }
        }
    }


    /**
     * 获取表信息
     *
     * @param dataSource 数据源
     * @param tableName  表名
     */
    private Table getTable(@NonNull DataSource dataSource, String tableName) {
        if (CharSequenceUtil.isBlank(tableName)) {
            return null;
        }
        Table table = null;
        try {
            table = MetaUtil.getTableMeta(dataSource, tableName);
            if (!this.isTableExists(table)) {
                table = MetaUtil.getTableMeta(dataSource, tableName.toUpperCase());
            }
        } catch (Exception ignored) {

        }
        return table;
    }


    /**
     * 获取表记录数
     *
     * @param tableName 来源表对象
     * @param condition 数据过滤条件
     * @return 记录数
     */
    private long getTableRecordCount(String tableName, String condition) throws SQLException {
        String countSql = CharSequenceUtil.isBlank(condition) ? CharSequenceUtil.format(QUERY_COUNT, tableName) :
                CharSequenceUtil.format(QUERY_COUNT_WITH_CONDITION, tableName, condition);
        Number total = this.sourceProvider.getDb().queryNumber(countSql);
        return ObjectUtil.isNotNull(total) ? total.longValue() : 0;
    }


    /**
     * 使用分页插入数据
     *
     * @param db          数据库对象
     * @param fromTable   来源表对象
     * @param toTable     目标表对象
     * @param recordCount 数据记录数
     * @param condition   数据过滤条件
     */
    private void insertDataWithPage(Db db, Table fromTable, Table toTable, long recordCount, String condition) throws SQLException {
        String querySql = CharSequenceUtil.isBlank(condition) ? CharSequenceUtil.format(QUERY_ALL, fromTable.getTableName()) :
                CharSequenceUtil.format(QUERY_WITH_CONDITION, fromTable.getTableName(), condition);
        Map<String, String> map = this.getFiledMapping(fromTable, toTable);
        //delete all
        db.execute(CharSequenceUtil.format(DELETE_ALL, toTable.getTableName()));
        log.info("All Data were deleted from {}", toTable.getTableName());
        int pageCount = PageUtil.totalPage(recordCount, this.executeStrategy.getDataPageSize());
        for (int i = 0; i < pageCount; i++) {
            PageResult<Entity> pageResult = this.sourceProvider.getDb().page(querySql, new Page(i, this.executeStrategy.getDataPageSize()));
            if (ObjectUtil.isNotEmpty(pageResult)) {
                List<Entity> toList = pageResult.stream().map(t -> this.convert(t, map)).collect(Collectors.toList());
                db.insert(toList);
                log.info("Table {} data transfer batch processing with page:{}/{} Page size:{}", toTable.getTableName(), i + 1, pageCount, this.executeStrategy.getDataPageSize());
            }
        }
        log.info("Table {} data transfer finished, total records:{}", toTable.getTableName(), recordCount);
    }


    /**
     * 删除数据并使用分页插入数据
     *
     * @param tableMeta 来源表对象
     * @param toTable   目标表对象
     * @param condition 数据过滤条件
     */
    private void insertData(TableMeta tableMeta, Table fromTable, Table toTable, String condition) throws SQLException {
        Db db = Db.use(this.connection);
        long recordCount;
        if (CharSequenceUtil.isNotBlank(condition)) {
            recordCount = this.getTableRecordCount(tableMeta.getTableName(), condition);
        } else {
            recordCount = tableMeta.getRecordCount();
        }
        this.insertDataWithPage(db, fromTable, toTable, recordCount, condition);
    }


    private Map<String, String> getFiledMapping(@NonNull Table fromnTable, @NonNull Table toTable) {
        Map<String, String> filedMapping = new HashMap<>(16);
        filedMapping.put("toTableName", toTable.getTableName());
        Collection<Column> fromFieldList = fromnTable.getColumns();
        Collection<Column> toFieldList = toTable.getColumns();
        for (Column fromField : fromFieldList) {
            toFieldList.stream()
                    .filter(t -> CharSequenceUtil.equalsIgnoreCase(t.getName(), fromField.getName()))
                    .findFirst()
                    .ifPresent(t -> filedMapping.put(fromField.getName().toLowerCase(), this.ddlProvider.wrapName(t.getName())));
        }
        return filedMapping;
    }

    private Entity convert(Entity fromEntity, Map<String, String> map) {
        Entity entity = new Entity();
        entity.setTableName(map.get("toTableName"));
        fromEntity.getFieldNames().forEach(t -> entity.set(map.get(t.toLowerCase()), fromEntity.get(t)));
        return entity;
    }
}
