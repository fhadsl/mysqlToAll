package com.haosmart.mysqltoall;

import com.haosmart.mysqltoall.utils.ConfigUtil;
import org.junit.Test;

import java.sql.SQLException;

public class ExecuteTest {

    @Test
    public void oracleTableTest() throws SQLException {
        DdlExecutor executor = new DdlExecutor(ConfigUtil.getConfig("mysqlFrom"), ConfigUtil.getConfig("oracleTo"));
        executor.syncSingleTable("tenant_config_meta");
    }

    @Test
    public void sqlServerTest() throws SQLException {
        DdlExecutor executor = new DdlExecutor(ConfigUtil.getConfig("mysqlFrom"), ConfigUtil.getConfig("sqlServerTo"));
        executor.syncSingleTable("tenant_config_meta");
    }

    @Test
    public void vastBaseTest() throws SQLException {
        DdlExecutor executor = new DdlExecutor(ConfigUtil.getConfig("mysqlFrom"), ConfigUtil.getConfig("vastBaseG100To"));
        executor.syncSingleTable("sys_license");
    }

    @Test
    public void kingBaseTest() throws SQLException {
        DdlExecutor executor = new DdlExecutor(ConfigUtil.getConfig("mysqlFrom"), ConfigUtil.getConfig("kingbase8To"));
        executor.syncSingleTable("sys_license");
    }

    @Test
    public void vaseBaseG100LogAllTest() throws SQLException {
        DdlExecutor executor = new DdlExecutor(ConfigUtil.getConfig("mysqlLogFrom"), ConfigUtil.getConfig("vastBaseG100To"));
        executor.syncAllTables();
    }


    @Test
    public void vaseBaseG100UploadAllTest() throws SQLException {
        DdlExecutor executor = new DdlExecutor(ConfigUtil.getConfig("mysqlUploadFrom"), ConfigUtil.getConfig("vastBaseG100To"));
        executor.syncAllTables();
    }


    @Test
    public void kingBaseBaseALLTest() throws SQLException {
        DdlExecutor executor = new DdlExecutor(ConfigUtil.getConfig("mysqlFrom"), ConfigUtil.getConfig("kingbase8To"));
        executor.syncAllTables(5);
    }


    @Test
    public void kingBaseLogALLTest() throws SQLException {
        DdlExecutor executor = new DdlExecutor(ConfigUtil.getConfig("mysqlLogFrom"), ConfigUtil.getConfig("kingbase8LogTo"));
        executor.syncAllTables();
    }


    @Test
    public void kingBaseUploadALLTest() throws SQLException {
        DdlExecutor executor = new DdlExecutor(ConfigUtil.getConfig("mysqlUploadFrom"), ConfigUtil.getConfig("kingbase8UploadTo"));
        executor.syncAllTables();
    }

    @Test
    public void kingBaseSeataALLTest() throws SQLException {
        DdlExecutor executor = new DdlExecutor(ConfigUtil.getConfig("mysqlSeataFrom"), ConfigUtil.getConfig("kingbase8SeataTo"));
        executor.syncAllTables();
    }
}
