package com.haosmart.mysqltoall.ddl.dialect;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.collection.ListUtil;
import cn.hutool.core.lang.Pair;
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
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * oracle DDL 提供者
 *
 * @author fujunhao
 */
@Slf4j
public class OracleDdlProvider extends AbstractDdlProvider {


    private static final String TABLE_COMMENT_FORMATTER = "COMMENT ON TABLE {} IS '{}'";

    private static final String COLUMN_COMMENT_FORMATTER = "COMMENT ON COLUMN {} IS '{}'";

    private static final String CREATE_PRIMARY_FORMATTER = "ALTER TABLE {} ADD PRIMARY KEY ({})";

    private static final String CREATE_SEARCH_JSON_INDEX = "CREATE SEARCH INDEX {} ON {} ({}) FOR JSON";

    private static final String SQL_SEPARATOR = ";";

    private static final String[] FUNCTIONS = {"CURRENT_TIMESTAMP"};

    private static final int MAX_NAME_LENGTH_STAND = 30;

    private static final int MAX_NAME_LENGTH_EXTEND = 32767;


    private final boolean isExtendedMode;

    private final boolean above12cr2;


    public OracleDdlProvider(DbConfig config, DatabaseMetaData databaseMetaData, CaseType caseType) {
        this(config, databaseMetaData, caseType, false);
    }

    public OracleDdlProvider(DbConfig config, DatabaseMetaData databaseMetaData, CaseType caseType, boolean isExtendedMode) {
        super(config, databaseMetaData, caseType);
        this.isExtendedMode = isExtendedMode;
        this.above12cr2 = isAbove12cr2();
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
     * 是否忽略该索引
     *
     * @param indexInfo 索引对象
     * @return 是否忽略该索引
     */
    @Override
    public boolean ignoredIndex(IndexInfo indexInfo) {
        return CharSequenceUtil.containsIgnoreCase(indexInfo.getIndexName(), "primary");
    }

    /**
     * 创建列注释DDL
     *
     * @param column 列对象
     * @return 注释DDL
     */
    @Override
    public String buildColumnComment(@NonNull Column column) {
        return CharSequenceUtil.format(COLUMN_COMMENT_FORMATTER, this.wrapNameWithSchema(String.join(COMMA, column.getTableName(), column.getName())), CharSequenceUtil.removeAll(column.getComment(), SQL_SEPARATOR));
    }


    /**
     * 创建表注释DDL
     *
     * @param table 表对象
     * @return 注释DDL
     */
    @Override
    public String getTableComment(Table table) {
        return CharSequenceUtil.format(TABLE_COMMENT_FORMATTER, this.wrapNameWithSchema(table.getTableName()), CharSequenceUtil.removeAll(table.getComment(), SQL_SEPARATOR));
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
        if (ObjectUtil.equals(actionType, ActionType.CREATE)) {
            String columName = indexInfo.getColumnIndexInfoList().stream()
                    .map(t -> this.removeBlank(t.getColumnName()).toUpperCase())
                    .collect(Collectors.joining("_"));
            String result = getINDEX_PREFIX() + CharSequenceUtil.join("_", this.removeBlank(indexInfo.getTableName().toUpperCase()), columName);
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
            //number
            case BIT:
            case BOOLEAN:
                return "NUMBER(1, 0)";
            case TINYINT:
            case SMALLINT:
                return "NUMBER(3,0)";
            case INTEGER:
                return "INTEGER";
            case BIGINT:
                return "NUMBER(20, 0)";
            case REAL:
            case DOUBLE:
                return "FLOAT(24)";
            case DECIMAL:
                return "DECIMAL(" + column.getSize() + "," + column.getDigit() + ")";
            case NUMERIC:
                return "NUMERIC(" + column.getSize() + "," + column.getDigit() + ")";
            //date
            case DATE:
            case TIME:
            case TIME_WITH_TIMEZONE:
            case DATETIMEOFFSET:
                return "DATE";
            case TIMESTAMP:
                return "TIMESTAMP";
            //string //oracle会有空格问题
            case CHAR:
            case VARCHAR:
                return "VARCHAR2(" + Math.min((column.getSize() * getChineseLength()), this.isExtendedMode ? MAX_NAME_LENGTH_EXTEND : 4000) + ")";
            case BINARY:
            case VARBINARY:
            case LONGVARBINARY:
                return "BLOB";
            case LONGNVARCHAR:
            case LONGVARCHAR:
                if (CharSequenceUtil.equalsIgnoreCase("json", column.getTypeName().trim())) {
                    return "CLOB CHECK (" + this.removeBlank(column.getName().toUpperCase()) + " IS JSON)";
                }
                return "CLOB";
            default:
                String range = "";
                if (column.getSize() > 0) {
                    range += "(" + column.getSize();
                }
                if (ObjectUtil.isNotNull(column.getDigit())) {
                    range += ", " + column.getDigit();
                }
                range += ")";
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

    /**
     * 生成其他脚本
     *
     * @param sourceConfig 源数据源配置
     * @param targetConfig 目标数据源配置
     * @param ddlTable     需要生成ddl脚本的表对象
     * @return 脚本列表
     */
    @Override
    public List<String> buildOtherSql(DbConfig sourceConfig, DbConfig targetConfig, Table ddlTable) {
        List<String> stringList = super.buildOtherSql(sourceConfig, targetConfig, ddlTable);
        if (this.above12cr2) {
            CollectionUtil.addAll(stringList, this.createSearchJsonIndex(ddlTable));
        }
        String pkSql = this.createPrimaryKey(ddlTable);
        if (CharSequenceUtil.isNotBlank(pkSql)) {
            stringList.add(pkSql);
        }
        return stringList;
    }

    /**
     * 处理SQL异常
     *
     * @param e         异常
     * @param tableName 表名
     * @return 是否已处理
     */
    @Override
    public boolean handleThrowable(Throwable e, String tableName) {
        if (CharSequenceUtil.containsIgnoreCase(e.getMessage(), "ORA-01400")) {
            log.error("表[{}]数据某些字段可能含有空字符串，并且这些字段为非空字段，请检查！{}", tableName, e.getMessage());
            return true;
        } else if (CharSequenceUtil.containsIgnoreCase(e.getMessage(), "ORA-01418")) {
            return true;
        }
        return false;
    }


    private List<String> createSearchJsonIndex(@NonNull Table table) {
        if (ObjectUtil.isEmpty(table.getColumns())) {
            return ListUtil.empty();
        }
        List<Column> jsonList = table.getColumns().stream()
                .filter(t -> CharSequenceUtil.equalsIgnoreCase("json", t.getTypeName()))
                .collect(Collectors.toList());
        if (ObjectUtil.isEmpty(jsonList)) {
            return ListUtil.empty();
        }
        return jsonList.stream()
                .map(t -> {
                    String indexName = CharSequenceUtil.join("", getINDEX_PREFIX(), this.createIndexName(t));
                    return CharSequenceUtil.format(CREATE_SEARCH_JSON_INDEX, this.wrapNameWithSchema(indexName), this.wrapNameWithSchema(table.getTableName()), this.wrapName(t.getName()));
                })
                .collect(Collectors.toList());
    }

    private String createPrimaryKey(Table table) {
        if (ObjectUtil.isEmpty(table.getPkNames())) {
            return "";
        }
        return CharSequenceUtil.format(CREATE_PRIMARY_FORMATTER, this.wrapNameWithSchema(table.getTableName()), table.getPkNames().stream().map(this::wrapName).collect(Collectors.joining(",")));
    }


    private String createIndexName(@NonNull Column column) {
        String columName = this.removeBlank(column.getName());
        String result = CharSequenceUtil.join("_", this.removeBlank(column.getTableName().toUpperCase()), columName);
        if (result.length() > this.getMaxStringLength() - 3) {
            result = this.splitIndexName(result, column.getTableName());
        }
        return result;
    }

    private String splitIndexName(String indexName, String tableName) {
        String newName = indexName.substring(0, this.getMaxStringLength() - (getINDEX_PREFIX().length() + 3)) + "_" + RandomUtil.randomString(2);
        log.warn("Table[{}] index[{}] name length was greater than {}, the name was replaced with {}", tableName, indexName, this.getMaxStringLength(), newName);
        indexName = newName;
        return indexName;
    }

    private int getMaxStringLength() {
        if (this.isExtendedMode) {
            return MAX_NAME_LENGTH_EXTEND;
        }
        return MAX_NAME_LENGTH_STAND;
    }

    private int getChineseLength() {
        Charset charset = ObjectUtil.isNotNull(this.getConfig().getEncoding()) ? this.getConfig().getEncoding() : StandardCharsets.UTF_8;
        return CharSequenceUtil.byteLength("我", charset);
    }

    private boolean isAbove12cr2() {
        try {
            Pair<Integer, Integer> pair = this.getDatabaseVersion();
            return pair.getKey() > 12 || pair.getValue() >= 2;
        } catch (SQLException e) {
            log.error("getDatabaseVersion error", e);
        }
        return false;
    }
}
