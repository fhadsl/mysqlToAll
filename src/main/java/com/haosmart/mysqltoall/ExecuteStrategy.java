package com.haosmart.mysqltoall;


import cn.hutool.core.collection.ListUtil;

import java.util.List;

/**
 * 执行策略
 *
 * @author fujunhao
 */
public interface ExecuteStrategy {

    /**
     * 创建类型
     */
    enum BuildType {
        /**
         * 删除重建
         */
        DELETE_AND_REBUILD,
        /**
         * 存在时跳过创建
         */
        SKIP_WHEN_EXIST
    }


    /**
     * 获取创建类型,默认为{@link BuildType#DELETE_AND_REBUILD}
     *
     * @return 创建类型
     */
    default BuildType getBuildType() {
        return BuildType.DELETE_AND_REBUILD;
    }

    /**
     * 获取可接受的最大记录数，若实际数据量超过该记录数，则忽略该表；-1表示接受所有的表。默认为{@code -1}
     *
     * @return 最大记录数
     */
    default long getMaxRecordCount() {
        return -1L;
    }

    /**
     * 是否同步表数据，默认为{@code True}
     *
     * @return 是否同步表数据
     */
    default boolean isIncludeData() {
        return true;
    }

    /**
     * 获取忽略同步的表名列表，支持正则表达式
     *
     * @return 忽略同步的表名列表
     */
    default List<String> getIgnoredTableNames() {
        return ListUtil.empty();
    }

    /**
     * 遇到错误是否继续同步，默认为{@code True}
     *
     * @return 是否继续同步
     */
    default boolean continueWhenError() {
        return true;
    }

    /**
     * 是否开启调试模式，默认为{@code false}
     *
     * @return 是否开启调试模式
     */
    default boolean debugMode() {
        return false;
    }

    /**
     * 获取同步数据的分页大小，默认为{@code 5000}
     *
     * @return 同步数据的分页大小
     */
    default int getDataPageSize() {
        return 5000;
    }

}
