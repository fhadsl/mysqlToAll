package com.haosmart.mysqltoall.config;

import cn.hutool.core.lang.Assert;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.setting.Setting;
import com.haosmart.mysqltoall.enumration.DbType;
import lombok.Data;

import java.nio.charset.Charset;


/**
 * @author fujunhao
 */
@Data
public class DbConfig {

    private String id;

    private String dbUrl;

    private String userName;

    private String password;

    private DbType dbType;

    private String schemaName;

    private Charset encoding;

    private Integer bufferRows;

    private String tbSpaceDdl;

    public void setId(String id) {
        Assert.notBlank(id, "id不能为空");
        this.id = id;
    }


    public void setPassword(String password) {
        Assert.notBlank(password, "连接密码不能为空");
        this.password = password;
    }

    public void setUserName(String userName) {
        Assert.notBlank(userName, "连接用户名不能为空");
        this.userName = userName;
    }

    public void setDbUrl(String dbUrl) {
        Assert.notBlank(dbUrl, "连接URL不能为空");
        this.dbUrl = dbUrl;
    }

    public Setting toSetting() {
        return new Setting()
                .set("driver", ObjectUtil.isNotNull(this.dbType) ? this.dbType.getDriverClass(): null)
                .set("url", dbUrl)
                .set("user", userName)
                .set("pass", password);
    }
}
