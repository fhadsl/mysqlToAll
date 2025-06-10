package com.haosmart.mysqltoall.ddl;

import com.haosmart.mysqltoall.config.DbConfig;
import com.haosmart.mysqltoall.ddl.dialect.KingBaseDdlProvider;
import com.haosmart.mysqltoall.ddl.dialect.OracleDdlProvider;
import com.haosmart.mysqltoall.ddl.dialect.SqlServerDdlProvider;
import com.haosmart.mysqltoall.ddl.dialect.VastBaseG100DdlProvider;
import com.haosmart.mysqltoall.enumration.CaseType;
import lombok.NonNull;

import java.sql.DatabaseMetaData;


/**
 * ddl提供者工厂
 *
 * @author fujunhao
 */
public class DdlProviderFactory {

    public static DdlProvider build(@NonNull DbConfig config, DatabaseMetaData databaseMetaData) {
        switch (config.getDbType()) {
            case ORACLE:
                return new OracleDdlProvider(config, databaseMetaData, CaseType.UPPER, true);
            case SQLSERVER:
                return new SqlServerDdlProvider(config, databaseMetaData, CaseType.REMAIN);
            case VAST_BASE:
                return new VastBaseG100DdlProvider(config, databaseMetaData, CaseType.LOWER);
            case KING_BASE_V8:
                return new KingBaseDdlProvider(config, databaseMetaData, CaseType.LOWER);
            default:
                throw new IllegalArgumentException("不支持的方言" + config.getDbType());
        }
    }


}
