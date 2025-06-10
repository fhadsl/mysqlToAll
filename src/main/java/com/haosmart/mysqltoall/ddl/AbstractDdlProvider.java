package com.haosmart.mysqltoall.ddl;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.collection.ListUtil;
import cn.hutool.core.lang.Assert;
import cn.hutool.core.lang.Pair;
import cn.hutool.core.lang.Validator;
import cn.hutool.core.text.CharSequenceUtil;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.ReUtil;
import cn.hutool.db.meta.Column;
import cn.hutool.db.meta.IndexInfo;
import cn.hutool.db.meta.Table;
import com.haosmart.mysqltoall.config.DbConfig;
import com.haosmart.mysqltoall.enumration.ActionType;
import com.haosmart.mysqltoall.enumration.CaseType;
import com.haosmart.mysqltoall.enumration.CommentType;
import lombok.Getter;
import lombok.NonNull;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * sql脚本生成器抽象类
 *
 * @author fujunhao
 */
@Getter
public abstract class AbstractDdlProvider implements DdlProvider {

    /**
     * 数据源配置
     */
    @Getter
    private final DbConfig config;

    /**
     * 数据元信息
     */
    private final DatabaseMetaData databaseMetaData;

    /**
     * 大小写类型
     */
    private final CaseType caseType;


    protected static final String SEPARATOR = ",";


    @Getter
    private static final String INDEX_PREFIX = "IDX_";

    protected static final String COMMA = ".";

    private static final String[] FUNCTIONS = {"CURRENT_TIMESTAMP"};


    public AbstractDdlProvider(DbConfig config, DatabaseMetaData databaseMetaData, CaseType caseType) {
        this.config = config;
        this.databaseMetaData = databaseMetaData;
        this.caseType = caseType;
    }


    /**
     * 创建ddl脚本
     *
     * @param sourceConfig 源数据源配置
     * @param targetConfig 目标数据源配置
     * @param ddlTable     需要生成ddl脚本的表对象
     * @param actionType   脚本动作类型
     * @return 脚本列表
     */
    @Override
    public List<String> buildDdl(DbConfig sourceConfig, DbConfig targetConfig, Table ddlTable, @NonNull ActionType actionType) {
        switch (actionType) {
            case DELETE:
                return this.buildDdlWithDeleteAction(sourceConfig, targetConfig, ddlTable);
            case CREATE:
                return this.buildDdlWithCreateAction(sourceConfig, targetConfig, ddlTable);
            default:
                return ListUtil.empty();
        }
    }


    /**
     * 包装字段
     *
     * @param name 字段
     * @return 包装后的字段
     */
    @Override
    public String wrapName(String name) {
        name = this.removeBlank(name);
        if (CharSequenceUtil.isBlank(name)) {
            return "";
        }
        String symbol = CharSequenceUtil.isBlank(getNameWrapSymbol()) ? "" : getNameWrapSymbol();
        switch (this.caseType) {
            case LOWER:
                name = name.toLowerCase();
                break;
            case UPPER:
                name = name.toUpperCase();
                break;
            default:
                break;
        }
        if (CharSequenceUtil.isBlank(symbol)) {
            return name;
        }
        List<String> part = CharSequenceUtil.split(name, COMMA);
        if (CharSequenceUtil.contains(symbol, "%")) {
            return part.stream().map(t -> String.format(symbol, t)).collect(Collectors.joining(COMMA));
        } else {
            return part.stream().map(t -> CharSequenceUtil.wrap(t, symbol)).collect(Collectors.joining(COMMA));
        }
    }

    /**
     * 获取索引模版DDL
     *
     * @param actionType 动作类型
     * @return 索引模版DDL
     */
    public String getIndexTemplate(ActionType actionType) {
        switch (actionType) {
            case CREATE:
                return "CREATE INDEX {} ON {} ({})";
            case DELETE:
                return "DROP INDEX {}";
            default:
                return null;
        }
    }


    /**
     * 获取表结构模版DDL
     *
     * @param actionType 动作类型
     * @return 表结构模版DDL
     */
    public String getTableStructureTemplate(ActionType actionType) {
        switch (actionType) {
            case CREATE:
                return "CREATE TABLE {} ({})";
            case DELETE:
                return "DROP TABLE {}";
            default:
                return null;
        }
    }

    /**
     * 获取注释位置类型
     *
     * @return 注释位置类型
     */
    public abstract CommentType getCommentType();


    /**
     * 创建列注释DDL
     *
     * @param column 列对象
     * @return 注释DDL
     */
    public abstract String buildColumnComment(Column column);

    /**
     * 创建表注释DDL
     *
     * @param table 表对象
     * @return 注释DDL
     */
    public abstract String getTableComment(Table table);


    /**
     * 处理数据类型
     *
     * @param column 来源字段信息
     * @return 目标数据类型
     */
    public abstract String handleColumnType(@NonNull Column column);


    /**
     * 获取字段名称的包围符号
     *
     * @return 字段名称的包围符号
     */
    public abstract String getNameWrapSymbol();


    /**
     * 获取列的默认值
     *
     * @param column 列对象
     * @return 默认值DDL
     */
    public String getColumnDefaultValue(Column column) {
        String defaultValue = column.getColumnDef();
        if (CharSequenceUtil.isBlank(defaultValue)) {
            return "";
        }
        if (!CharSequenceUtil.containsAnyIgnoreCase(defaultValue, FUNCTIONS)
                && (Validator.hasChinese(defaultValue) || ReUtil.contains("[a-zA-Z]", defaultValue))) {
            defaultValue = CharSequenceUtil.wrap(defaultValue, "'");
        }
        return defaultValue;
    }

    /**
     * 是否忽略该索引
     *
     * @param indexInfo 索引对象
     * @return 是否忽略该索引
     */
    public boolean ignoredIndex(IndexInfo indexInfo) {
        return false;
    }


    /**
     * 获取表名
     *
     * @param targetConfig 目标数据源配置
     * @param ddlTable     待生成DDL的表对象
     * @return 表名
     */
    public String getTableName(DbConfig targetConfig, Table ddlTable) {
        return this.wrapName(ddlTable.getTableName());
    }


    /**
     * 获取索引名
     *
     * @param targetConfig 目标数据源配置
     * @param indexInfo    索引对象
     * @return 索引名
     */
    public String getIndexName(DbConfig targetConfig, IndexInfo indexInfo, ActionType actionType) {
        return this.wrapName(indexInfo.getIndexName());
    }

    /**
     * 创建表结构DDL
     *
     * @param sourceConfig      源数据源配置
     * @param targetConfig      目标数据源配置
     * @param ddlTable          待生成DDL的表对象
     * @param structureTemplate 表结构模版
     * @return 表结构DDL
     */
    public String buildStructureDdl(DbConfig sourceConfig, DbConfig targetConfig, Table ddlTable, String structureTemplate) {
        Collection<Column> columns = ddlTable.getColumns();
        Assert.notEmpty(columns, "待创建的表结构{}字段为空，请检查", ddlTable.getTableName());
        String columnDdl = this.buildColumnDdl(columns);
        String structureDdl = CharSequenceUtil.format(structureTemplate, this.getTableName(targetConfig, ddlTable), columnDdl);
        if (ObjectUtil.equals(CommentType.INTERNAL, this.getCommentType())) {
            structureDdl = String.join(" ", structureDdl, this.getTableComment(ddlTable));
        }
        return structureDdl;
    }

    /**
     * 创建列DDL
     *
     * @param columns 列对象集合
     * @return 列DDL
     */
    public String buildColumnDdl(Collection<Column> columns) {
        return columns.stream().map(this::buildColumnDdl).collect(Collectors.joining(SEPARATOR));
    }


    /**
     * 创建列DDL
     *
     * @param column 列对象
     * @return 列DDL
     */
    public String buildColumnDdl(Column column) {
        String defaultValue = this.getColumnDefaultValue(column);
        if (CharSequenceUtil.isBlank(defaultValue)) {
            defaultValue = "";
        } else {
            defaultValue = " DEFAULT " + defaultValue;
        }
        return this.wrapName(column.getName()) +
                " " +
                this.handleColumnType(column) +
                " " + defaultValue +
                " " +
                (column.isNullable() ? "NULL" : "NOT NULL") +
                " " +
                (ObjectUtil.equals(CommentType.INTERNAL, this.getCommentType()) ? this.buildColumnComment(column) : "");
    }


    /**
     * 获取数据库版本号
     *
     * @return 数据库版本号  (主版本号，次版本号)
     */
    public Pair<Integer, Integer> getDatabaseVersion() throws SQLException {
        return new Pair<>(this.databaseMetaData.getDatabaseMajorVersion(), this.databaseMetaData.getDatabaseMinorVersion());
    }


    /**
     * 删除不可见字符
     *
     * @param str 字符
     * @return 删除后的字符
     */
    public String removeBlank(String str) {
        return CharSequenceUtil.isBlank(str) ? str : str.replaceAll("\\s+", "");
    }

    /**
     * 生成其他脚本
     *
     * @param sourceConfig 源数据源配置
     * @param targetConfig 目标数据源配置
     * @param ddlTable     需要生成ddl脚本的表对象
     * @return 脚本列表
     */
    public List<String> buildOtherSql(DbConfig sourceConfig, DbConfig targetConfig, Table ddlTable) {
        if (ObjectUtil.equals(this.getCommentType(), CommentType.EXTERNAL)) {
            List<String> columCommentList = ddlTable.getColumns().stream()
                    .map(this::buildColumnComment)
                    .filter(CharSequenceUtil::isNotBlank)
                    .collect(Collectors.toList());
            List<String> ddlList = ListUtil.toList(columCommentList);
            String tableComment = this.getTableComment(ddlTable);
            if (CharSequenceUtil.isNotBlank(tableComment)) {
                ddlList.add(tableComment);
            }
            return ddlList;
        }
        return ListUtil.empty();
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
        return false;
    }

    /**
     * 使用schema名称包装name
     *
     * @param name 需要包装的字段
     * @return 包装后的字段
     */
    protected String wrapNameWithSchema(String name) {
        if (CharSequenceUtil.isNotBlank(this.getConfig().getSchemaName())) {
            return this.wrapName(CharSequenceUtil.join(COMMA, this.getConfig().getSchemaName(), name));
        }
        return this.wrapName(name);
    }


    private List<String> buildDdlWithCreateAction(DbConfig sourceConfig, DbConfig targetConfig, @NonNull Table ddlTable) {
        List<String> ddlList = ListUtil.toList(this.buildStructureDdl(sourceConfig, targetConfig, ddlTable, ActionType.CREATE));
        List<String> indexDdlList = this.buildIndexDdl(targetConfig, ddlTable, ActionType.CREATE);
        CollectionUtil.addAll(ddlList, indexDdlList);
        List<String> otherDdlList = this.buildOtherSql(sourceConfig, targetConfig, ddlTable);
        CollectionUtil.addAll(ddlList, otherDdlList);
        return ddlList;
    }

    private List<String> buildIndexDdl(DbConfig targetConfig, @NonNull Table ddlTable, ActionType actionType) {
        List<IndexInfo> indexInfoList = ddlTable.getIndexInfoList();
        if (ObjectUtil.isEmpty(indexInfoList)) {
            return ListUtil.empty();
        }
        return indexInfoList.stream()
                .filter(t -> !this.ignoredIndex(t))
                .map(t -> this.buildIndexSql(targetConfig, ddlTable, t, actionType))
                .filter(CharSequenceUtil::isAllNotBlank)
                .collect(Collectors.toList());
    }


    private List<String> buildDdlWithDeleteAction(DbConfig sourceConfig, DbConfig targetConfig, @NonNull Table ddlTable) {
        List<String> indexDdlList = this.buildIndexDdl(targetConfig, ddlTable, ActionType.DELETE);
        List<String> ddlList = new ArrayList<>(16);
        CollectionUtil.addAll(ddlList, indexDdlList);
        ddlList.add(this.buildStructureDdl(sourceConfig, targetConfig, ddlTable, ActionType.DELETE));
        return ddlList;
    }

    /**
     * 生成表结构脚本
     *
     * @param sourceConfig 源数据源配置
     * @param targetConfig 目标数据源配置
     * @param ddlTable     需要生成ddl脚本的表对象
     * @param actionType   脚本动作类型
     * @return 脚本列表
     */
    private String buildStructureDdl(DbConfig sourceConfig, DbConfig targetConfig, Table ddlTable, ActionType actionType) {
        String template = this.getTableStructureTemplate(actionType);
        switch (actionType) {
            case CREATE:
                return this.buildStructureDdl(sourceConfig, targetConfig, ddlTable, template);
            case DELETE:
                return CharSequenceUtil.format(template, this.getTableName(targetConfig, ddlTable));
            default:
                return null;
        }
    }


    /**
     * 生成表索引脚本
     *
     * @param targetConfig 目标数据源配置
     * @param indexInfo    源索引信息
     * @return 脚本列表
     */
    private String buildIndexSql(DbConfig targetConfig, @NonNull Table ddlTable, @NonNull IndexInfo indexInfo, ActionType actionType) {
        String template = this.getIndexTemplate(actionType);
        switch (actionType) {
            case CREATE:
                String fieldList = CharSequenceUtil.join(SEPARATOR,
                        indexInfo.getColumnIndexInfoList()
                                .stream()
                                .map(t -> this.wrapName(t.getColumnName()))
                                .filter(CharSequenceUtil::isNotBlank)
                                .collect(Collectors.toList()));
                return CharSequenceUtil.format(template, this.getIndexName(targetConfig, indexInfo, actionType), this.getTableName(targetConfig, ddlTable), fieldList);
            case DELETE:
                return CharSequenceUtil.format(template, this.getIndexName(targetConfig, indexInfo, actionType));
            default:
                return null;
        }
    }

}
