package com.haosmart.mysqltoall.enumration;

import cn.hutool.core.text.CharSequenceUtil;
import lombok.Getter;

import java.util.Arrays;

/**
 * 数据库类型
 *
 * @author fujunhao
 */

@Getter
public enum DbType {

    MYSQL("mysql", "com.mysql.cj.jdbc.Driver"),

    ORACLE("oracle", "oracle.jdbc.driver.OracleDriver"),

    SQLSERVER("sqlserver", "com.microsoft.sqlserver.jdbc.SQLServerDriver"),

    POSTGRESQL("postgresql", "org.postgresql.Driver"),

    /**
     * 海量数据库
     * */
    VAST_BASE("vastbase", "cn.com.vastbase.Driver"),

    /**
     * 电科金仓 V8
     * */
    KING_BASE_V8("kingbase8", "com.kingbase8.Driver");

    private final String name;

    private final String driverClass;

    DbType(String name, String driverClass) {
        this.name = name;
        this.driverClass = driverClass;
    }

    public static DbType of(String name) {
        return Arrays.stream(DbType.values())
                .filter(s -> CharSequenceUtil.equalsIgnoreCase(name, s.getName()))
                .findFirst()
                .orElse(null);
    }
}
