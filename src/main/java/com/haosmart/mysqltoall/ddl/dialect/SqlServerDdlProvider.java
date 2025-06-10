package com.haosmart.mysqltoall.ddl.dialect;

import cn.hutool.core.lang.Validator;
import cn.hutool.core.text.CharSequenceUtil;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.RandomUtil;
import cn.hutool.core.util.ReUtil;
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

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.DatabaseMetaData;
import java.util.stream.Collectors;

@Slf4j
public class SqlServerDdlProvider extends AbstractDdlProvider {

    private static final String CHECK_IS_JSON = "NVARCHAR(4000) CONSTRAINT {} CHECK (ISJSON([{}]) = 1)";

    private static final String TABLE_COMMENT_FORMATTER = "EXEC SP_ADDEXTENDEDPROPERTY 'MS_Description', {}, 'SCHEMA', '{}', 'TABLE', '{}'";

    private static final String COLUMN_COMMENT_FORMATTER = "EXEC SP_ADDEXTENDEDPROPERTY 'MS_Description', {}, 'SCHEMA', '{}', 'TABLE', '{}', 'COLUMN', '{}'";

    private static final String[] FUNCTIONS = {"CURRENT_TIMESTAMP"};

    private static final int NVARCHAR_MAX_LENGTH = 4000;

    private static final int MAX_NAME_LENGTH_STAND = 128;

    public SqlServerDdlProvider(DbConfig config, DatabaseMetaData databaseMetaData, CaseType caseType) {
        super(config, databaseMetaData, caseType);
    }

    /**
     * 获取注释位置类型
     *
     * @return 注释位置类型
     */
    @Override
    public CommentType getCommentType() {
        return CommentType.EXTERNAL;
    }

    /**
     * 创建列注释DDL
     *
     * @param column 列对象
     * @return 注释DDL
     */
    @Override
    public String buildColumnComment(Column column) {
        return CharSequenceUtil.format(COLUMN_COMMENT_FORMATTER, this.wrapChinese(column.getComment()), this.getConfig().getSchemaName(), column.getTableName(), column.getName());
    }


    /**
     * 是否忽略该索引
     *
     * @param indexInfo 索引对象
     * @return 是否忽略该索引
     */
    @Override
    public boolean ignoredIndex(IndexInfo indexInfo) {
        return CharSequenceUtil.containsIgnoreCase(indexInfo.getIndexName(), "pk") || CharSequenceUtil.startWithIgnoreCase(indexInfo.getIndexName(), "sys_");
    }

    /**
     * 创建表注释DDL
     *
     * @param table 表对象
     * @return 注释DDL
     */
    @Override
    public String getTableComment(Table table) {
        return CharSequenceUtil.format(TABLE_COMMENT_FORMATTER, this.wrapChinese(table.getComment()), this.getConfig().getSchemaName(), table.getTableName());
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
     * 获取索引名
     *
     * @param targetConfig 目标数据源配置
     * @param indexInfo    索引对象
     * @return 索引名
     */
    @Override
    public String getIndexName(DbConfig targetConfig, IndexInfo indexInfo, ActionType actionType) {
        if(ObjectUtil.equals(actionType, ActionType.CREATE)) {
            String columName = indexInfo.getColumnIndexInfoList().stream()
                .map(t -> this.removeBlank(t.getColumnName()))
                .collect(Collectors.joining("_"));
        String result = getINDEX_PREFIX() + CharSequenceUtil.join("_", this.removeBlank(indexInfo.getTableName()), columName);
        if (result.length() > this.getMaxStringLength() - getINDEX_PREFIX().length()) {
            result = this.splitIndexName(result, indexInfo.getTableName());
        }
        return this.wrapNameWithSchema(result);
        }
        return this.wrapNameWithSchema(String.join(COMMA, indexInfo.getTableName(), indexInfo.getIndexName()));
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
            //number
            case BIT:
            case BOOLEAN:
                return "BIT";
            case INTEGER:
                return "INT";
            case BIGINT:
                return "BIGINT";
            case REAL:
                return "FLOAT(24)";
            case DOUBLE:
                return "FLOAT(53)";
            case DECIMAL:
            case NUMERIC:
                return "DECIMAL(" + column.getSize() + "," + column.getDigit() + ")";
            //date
            case TIME_WITH_TIMEZONE:
            case DATETIMEOFFSET:
                return "DATETIMEOFFSET(3)";
            case TIMESTAMP:
                return "DATETIME";
            //string //oracle会有空格问题
            case CHAR:
            case VARCHAR:
                Object num = this.getVarcharMaxLength(column) > NVARCHAR_MAX_LENGTH ? "MAX" : this.getVarcharMaxLength(column);
                return CharSequenceUtil.format("NVARCHAR({})", num);
            case LONGVARBINARY:
                return "IMAGE";
            case LONGNVARCHAR:
            case LONGVARCHAR:
                if (CharSequenceUtil.equalsIgnoreCase("json", column.getTypeName().trim())) {
                    return CharSequenceUtil.format(CHECK_IS_JSON, "json_" + String.join("_", this.removeBlank(column.getTableName()), this.removeBlank(column.getName())), this.removeBlank(column.getName()));
                }
                return "NVARCHAR(MAX)";
            default:
                return column.getTypeName().toUpperCase();
        }
    }

    /**
     * 获取字段名称的包围符号
     *
     * @return 字段名称的包围符号
     */
    @Override
    public String getNameWrapSymbol() {
        return "[%s]";
    }


    /**
     * 创建列DDL
     *
     * @param column 列对象
     * @return 列DDL
     */
    @Override
    public String buildColumnDdl(Column column) {
        return super.buildColumnDdl(column) + " " + (column.isPk() ? "PRIMARY KEY" : "");
    }


    private String splitIndexName(String indexName, String tableName) {
        String newName = indexName.substring(0, this.getMaxStringLength() - (getINDEX_PREFIX().length() + 3)) + "_" + RandomUtil.randomString(2);
        log.warn("Table[{}] index[{}] name length was greater than {}, the name was replaced with {}", tableName, indexName, this.getMaxStringLength(), newName);
        indexName = newName;
        return indexName;
    }

    private long getVarcharMaxLength(@NonNull Column column) {
        return column.getSize() * getChineseLength();
    }

    private int getChineseLength() {
        Charset charset = ObjectUtil.isNotNull(this.getConfig().getEncoding()) ? this.getConfig().getEncoding() : StandardCharsets.UTF_8;
        return CharSequenceUtil.byteLength("我", charset);
    }

    private int getMaxStringLength() {
        return MAX_NAME_LENGTH_STAND;
    }


    private String wrapChinese(String value) {
        if (!CharSequenceUtil.isWrap(value, "'")) {
            value = CharSequenceUtil.wrap(value, "'");
        }
        if (Validator.hasChinese(value)) {
            return "N" + value;
        }
        return value;
    }
}
