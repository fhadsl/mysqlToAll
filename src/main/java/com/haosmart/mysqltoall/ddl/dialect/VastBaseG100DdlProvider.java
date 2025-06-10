package com.haosmart.mysqltoall.ddl.dialect;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.text.CharSequenceUtil;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.RandomUtil;
import cn.hutool.db.meta.Column;
import cn.hutool.db.meta.IndexInfo;
import cn.hutool.db.meta.JdbcType;
import cn.hutool.db.meta.Table;
import com.haosmart.mysqltoall.config.DbConfig;
import com.haosmart.mysqltoall.ddl.AbstractDdlProvider;
import com.haosmart.mysqltoall.enumration.ActionType;
import com.haosmart.mysqltoall.enumration.CaseType;
import com.haosmart.mysqltoall.enumration.CommentType;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.sql.DatabaseMetaData;
import java.util.Collection;
import java.util.stream.Collectors;


@Slf4j
public class VastBaseG100DdlProvider extends AbstractDdlProvider {


    private static final String CREATE_PRIMARY_FORMATTER = "CONSTRAINT {} PRIMARY KEY ({})";


    private static final String COMMENT_FORMATTER = "COMMENT '{}'";


    private static final String SQL_SEPARATOR = ";";

    private static final int MAX_NAME_LENGTH_STAND = 128;


    public VastBaseG100DdlProvider(DbConfig config, DatabaseMetaData databaseMetaData, CaseType caseType) {
        super(config, databaseMetaData, caseType);
    }

    /**
     * 获取注释位置类型
     *
     * @return 注释位置类型
     */
    @Override
    public CommentType getCommentType() {
        return CommentType.INTERNAL;
    }

    /**
     * 创建列注释DDL
     *
     * @param column 列对象
     * @return 注释DDL
     */
    @Override
    public String buildColumnComment(Column column) {
        return CharSequenceUtil.format(COMMENT_FORMATTER, CharSequenceUtil.removeAll(column.getComment(), SQL_SEPARATOR));
    }


    /**
     * 是否忽略该索引
     *
     * @param indexInfo 索引对象
     * @return 是否忽略该索引
     */
    @Override
    public boolean ignoredIndex(IndexInfo indexInfo) {
        return CharSequenceUtil.containsIgnoreCase(indexInfo.getIndexName(), "_pkey");
    }


    /**
     * 创建列DDL
     *
     * @param columns 列对象集合
     * @return 列DDL
     */
    @Override
    public String buildColumnDdl(Collection<Column> columns) {
        return super.buildColumnDdl(columns) + this.createPrimaryKey(columns);
    }

    /**
     * 获取表名
     *
     * @param targetConfig 目标数据源配置
     * @param ddlTable     待生成DDL的表对象
     * @return 表名
     */
    @Override
    public String getTableName(DbConfig targetConfig, Table ddlTable) {
        return this.wrapNameWithSchema(ddlTable.getTableName());
    }

    /**
     * 创建表注释DDL
     *
     * @param table 表对象
     * @return 注释DDL
     */
    @Override
    public String getTableComment(Table table) {
        return CharSequenceUtil.format(COMMENT_FORMATTER, CharSequenceUtil.removeAll(table.getComment(), SQL_SEPARATOR));
    }


    /**
     * 获取索引名
     *
     * @param targetConfig 目标数据源配置
     * @param indexInfo    索引对象
     * @return 索引名
     */
    @Override
    public String getIndexName(DbConfig targetConfig, IndexInfo indexInfo, ActionType actionType) {
        if (ObjectUtil.equals(actionType, ActionType.CREATE)) {
            String columName = indexInfo.getColumnIndexInfoList().stream()
                    .map(t -> this.removeBlank(t.getColumnName()))
                    .collect(Collectors.joining("_"));
            String result = getINDEX_PREFIX().toLowerCase() + CharSequenceUtil.join("_", this.removeBlank(indexInfo.getTableName()), columName);
            if (result.length() > this.getMaxStringLength() - getINDEX_PREFIX().length()) {
                result = this.splitIndexName(result, indexInfo.getTableName());
            }
            return this.wrapNameWithSchema(result);
        }
        return this.wrapNameWithSchema(indexInfo.getIndexName());
    }

    /**
     * 处理数据类型
     *
     * @param column 来源字段信息
     * @return 目标数据类型
     */
    @Override
    public String handleColumnType(@NonNull Column column) {
        JdbcType jdbcType = column.getTypeEnum();
        switch (jdbcType) {
            case BIT:
            case BOOLEAN:
            case TINYINT:
                return "TINYINT";
            case SMALLINT:
                return "SMALLINT";
            case INTEGER:
                return "INTEGER";
            case BIGINT:
                return "BIGINT";
            case DOUBLE:
                return "DOUBLE";
            case DECIMAL:
                return "DECIMAL" + CharSequenceUtil.wrap(CharSequenceUtil.join(",", column.getSize(), column.getDigit()), "(", ")");
            case NUMERIC:
                return "NUMERIC" + CharSequenceUtil.wrap(CharSequenceUtil.join(",", column.getSize(), column.getDigit()), "(", ")");
            case DATE:
                return "DATE";
            case BLOB:
            case LONGVARBINARY:
                return "BLOB";
            case LONGNVARCHAR:
            case LONGVARCHAR:
                if (CharSequenceUtil.equalsIgnoreCase("json", column.getTypeName().trim())) {
                    return "TEXT";
                }
            default:
                String range = "";
                if (column.getSize() > 0) {
                    range += "(" + column.getSize();
                }
                if (ObjectUtil.isNotNull(column.getDigit()) && column.getDigit() > 0) {
                    range += ", " + column.getDigit();
                }
                if (CharSequenceUtil.isNotBlank(range)) {
                    range += ")";
                }
                return column.getTypeName() + range;
        }
    }

    /**
     * 获取字段名称的包围符号
     *
     * @return 字段名称的包围符号
     */
    @Override
    public String getNameWrapSymbol() {
        return "\"";
    }

    private String createPrimaryKey(Collection<Column> columns) {
        if (ObjectUtil.isEmpty(columns)) {
            return "";
        }
        String pkIdListStr = columns.stream().filter(Column::isPk).map(Column::getName).collect(Collectors.joining(","));
        if(CharSequenceUtil.isBlank(pkIdListStr)) {
            return "";
        }
        return SEPARATOR + CharSequenceUtil.format(CREATE_PRIMARY_FORMATTER, this.wrapName(CollectionUtil.getFirst(columns).getTableName() + "_pkey"), columns.stream().filter(Column::isPk).map(Column::getName).collect(Collectors.joining(",")));
    }

    private String splitIndexName(String indexName, String tableName) {
        String newName = indexName.substring(0, this.getMaxStringLength() - (getINDEX_PREFIX().length() + 3)) + "_" + RandomUtil.randomString(2);
        log.warn("Table[{}] index[{}] name length was greater than {}, the name was replaced with {}", tableName, indexName, this.getMaxStringLength(), newName);
        return newName;
    }


    private int getMaxStringLength() {
        return MAX_NAME_LENGTH_STAND;
    }
}
