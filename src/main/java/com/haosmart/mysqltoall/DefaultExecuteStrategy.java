package com.haosmart.mysqltoall;

/**
 * @author fujunhao
 */
public class DefaultExecuteStrategy implements ExecuteStrategy {

    /**
     * 获取可接受的最大记录数，若实际数据量超过该记录数，则忽略该表；-1表示接受所有的表。默认为{@code -1}
     *
     * @return 最大记录数
     */
    @Override
    public long getMaxRecordCount() {
        return 50 * 10000L;
    }
}
