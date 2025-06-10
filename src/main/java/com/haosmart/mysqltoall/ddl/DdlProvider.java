package com.haosmart.mysqltoall.ddl;

import cn.hutool.db.meta.Table;
import com.haosmart.mysqltoall.config.DbConfig;
import com.haosmart.mysqltoall.enumration.ActionType;

import java.util.List;


/**
 * ddl提供者
 *
 * @author fujunhao
 */
public interface DdlProvider {


    /**
     * 创建ddl脚本
     *
     * @param sourceConfig 源数据源配置
     * @param targetConfig 目标数据源配置
     * @param ddlTable     需要生成ddl脚本的表对象
     * @param actionType   脚本动作类型
     * @return 脚本列表
     */
    List<String> buildDdl(DbConfig sourceConfig, DbConfig targetConfig, Table ddlTable, ActionType actionType);

    /**
     * 包装字段
     *
     * @param name 字段
     * @return 包装后的字段
     */
    String wrapName(String name);



    /**
     * 处理SQL异常
     *
     * @param e         异常
     * @param tableName 表名
     * @return 是否已处理
     */
    boolean handleThrowable(Throwable e, String tableName);
}
