package com.haosmart.mysqltoall;

import cn.hutool.db.Db;
import cn.hutool.db.DbUtil;
import cn.hutool.db.ds.druid.DruidDSFactory;
import cn.hutool.setting.Setting;
import com.haosmart.mysqltoall.config.DbConfig;
import lombok.AllArgsConstructor;
import lombok.Getter;

import javax.sql.DataSource;

@Getter
@AllArgsConstructor
public class DataSourceProvider {

    private final DbConfig config;
    private final DataSource dataSource;
    private final Db db;

    private DataSourceProvider(DataSource dataSource, DbConfig config) {
        this.dataSource = dataSource;
        this.config = config;
        this.db = DbUtil.use(dataSource);
    }

    public static DataSourceProvider create(DbConfig config) {
        Setting setting = config.toSetting();
        setting.set("maxActive", "10");
        setting.set("minIdle", "10");
        setting.set("initialSize", "5");
        try (DruidDSFactory dsFactory = new DruidDSFactory(setting)) {
            return new DataSourceProvider(dsFactory.getDataSource(), config);
        }
    }
}
