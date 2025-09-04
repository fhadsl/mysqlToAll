package com.haosmart.mysqltoall;

import cn.hutool.core.collection.ListUtil;
import cn.hutool.core.text.CharSequenceUtil;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.db.meta.MetaUtil;
import com.haosmart.mysqltoall.config.DbConfig;
import com.haosmart.mysqltoall.ddl.DdlProvider;
import com.haosmart.mysqltoall.ddl.DdlProviderFactory;
import com.haosmart.mysqltoall.entity.TableMeta;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@Slf4j
public class DdlExecutor {

    private final DdlProvider provider;

    private final DataSourceProvider sourceProvider;

    private final DataSourceProvider targetProvider;

    private final ExecuteStrategy executeStrategy;

    private final static String QUERY_COUNT = "select count(*) from {}";


    public DdlExecutor(DbConfig sourceConfig, DbConfig targetConfig, ExecuteStrategy executeStrategy) throws SQLException {
        this.sourceProvider = DataSourceProvider.create(sourceConfig);
        this.targetProvider = DataSourceProvider.create(targetConfig);
        this.executeStrategy = executeStrategy;
        // Use a temporary connection to get metadata, then close it.
        try (Connection metaConn = this.targetProvider.getDataSource().getConnection()) {
            this.provider = DdlProviderFactory.build(targetConfig, metaConn.getMetaData());
        }
    }

    public DdlExecutor(DbConfig sourceConfig, DbConfig targetConfig) throws SQLException {
        this(sourceConfig, targetConfig, new DefaultExecuteStrategy());
    }

    public void syncSingleTable(String tableName, String condition) throws Exception {
        List<TableMeta> tableList = this.getTableMetas(tableName);
        if (ObjectUtil.isEmpty(tableList)) {
            return;
        }

        Connection conn = null;
        try {
            conn = this.targetProvider.getDataSource().getConnection();
            conn.setAutoCommit(false);
            DdlExecutorWorker ddlExecutorWorker = this.createWorker(conn);
            ddlExecutorWorker.transferSingleTable(tableList.get(0), condition);
            conn.commit();
            log.warn(Thread.currentThread().getName() + " finished");
        } catch (Exception e) {
            if (conn != null) {
                conn.rollback();
            }
            throw e;
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }


    public void syncSingleTable(String tableName) throws Exception {
        this.syncSingleTable(tableName, null);
    }


    public void syncSingleTableList(String... tableNames) {
        this.syncSingleTableList(1, tableNames);
    }

    public void syncSingleTableList(int workerCount, String... tableNames) {
        if (ObjectUtil.isEmpty(tableNames)) {
            return;
        }
        List<List<TableMeta>> tableList = this.loadBalanceTables(workerCount, tableNames);
        this.executedByWorker(tableList);
    }

    public void syncAllTables(int workerCount) {
        List<List<TableMeta>> tableList = this.loadBalanceTables(workerCount);
        this.executedByWorker(tableList);
    }

    public void syncAllTables() {
        this.syncAllTables(15);
    }

    private void executedByWorker(List<List<TableMeta>> tableList) {
        if (ObjectUtil.isEmpty(tableList)) {
            return;
        }
        Connection conn = null;
        try {
            conn = this.targetProvider.getDataSource().getConnection();
            conn.setAutoCommit(false);
            final Connection finalConn = conn;

            List<CompletableFuture<Void>> futures = new ArrayList<>(16);
            for (List<TableMeta> fragment : tableList) {
                CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                    try {
                        DdlExecutorWorker ddlExecutorWorker = this.createWorker(finalConn);
                        ddlExecutorWorker.transferTableList(fragment);
                        log.warn(Thread.currentThread().getName() + " finished");
                    } catch (Throwable e) {
                        throw new RuntimeException(e);
                    }
                });
                futures.add(future);
            }

            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
            conn.commit();
            log.warn("All workers finished successfully, transaction committed.");

        } catch (Exception e) {
            log.error("Error during concurrent execution, rolling back transaction.", e);
            if (conn != null) {
                try {
                    conn.rollback();
                } catch (SQLException ex) {
                    log.error("Error during transaction rollback.", ex);
                }
            }
            throw new RuntimeException(e);
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    log.error("Error closing connection.", e);
                }
            }
        }
    }


    private DdlExecutorWorker createWorker(Connection connection) {
        return new DdlExecutorWorker(this.sourceProvider, this.targetProvider, this.provider, this.executeStrategy, connection);
    }

    private List<TableMeta> getTableMetas(String... tableNames) {
        List<String> tableList = MetaUtil.getTables(this.sourceProvider.getDataSource());
        if (ObjectUtil.isEmpty(tableList)) {
            return ListUtil.empty();
        }
        if (ObjectUtil.isNotEmpty(tableNames)) {
            tableList = tableList.stream().filter(t -> CharSequenceUtil.containsAny(t, tableNames)).collect(Collectors.toList());
        }
        return this.createTableMetaList(tableList);
    }


    private List<List<TableMeta>> loadBalanceTables(int executeWorkerCount, String... tableNames) {
        List<TableMeta> tableMetaList = this.getTableMetas(tableNames);
        if (ObjectUtil.isEmpty(tableMetaList)) {
            return ListUtil.empty();
        }
        //倒序
        tableMetaList.sort(Comparator.reverseOrder());

        if (executeWorkerCount > tableMetaList.size()) {
            executeWorkerCount = tableMetaList.size();
        }
        List<List<TableMeta>> result = new ArrayList<>(16);
        for (int i = 0; i < executeWorkerCount; i++) {
            result.add(new ArrayList<>());
        }
        int groupSize = tableMetaList.size() / executeWorkerCount;
        if (tableMetaList.size() % executeWorkerCount != 0) {
            groupSize++;
        }
        for (int i = 0; i < groupSize; i++) {
            for (int j = 0; j < executeWorkerCount; j++) {
                int index = i * executeWorkerCount + j;
                if (index >= tableMetaList.size()) {
                    continue;
                }
                result.get(j).add(tableMetaList.get(index));
            }
        }
        return result;
    }


    private List<TableMeta> createTableMetaList(List<String> tableList) {

        List<TableMeta> tableMetas = new ArrayList<>(16);
        for (String tableName : tableList) {
            TableMeta tableMeta = this.createTableMeta(tableName);
            if (ObjectUtil.isNull(tableMeta)) {
                continue;
            }
            tableMetas.add(tableMeta);
        }
        return tableMetas;
    }

    private TableMeta createTableMeta(String tableName) {

        String countSql = CharSequenceUtil.format(QUERY_COUNT, tableName);
        try {
            Number count = this.sourceProvider.getDb().queryNumber(countSql);
            long recordCount = ObjectUtil.isNotNull(count) ? count.longValue() : 0;
            if (this.executeStrategy.getMaxRecordCount() == -1 || recordCount < this.executeStrategy.getMaxRecordCount()) {
                return new TableMeta(recordCount, tableName);
            } else {
                log.warn("Table {} data transfer skipped, total records:{} exceed max:{}", tableName, recordCount, this.executeStrategy.getMaxRecordCount());
            }
        } catch (Exception ignored) {
        }
        return null;
    }

}
