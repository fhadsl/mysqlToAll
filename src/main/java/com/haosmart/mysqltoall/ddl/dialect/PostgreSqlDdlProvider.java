package com.haosmart.mysqltoall.ddl.dialect;

import cn.hutool.core.text.CharSequenceUtil;
import cn.hutool.db.meta.Column;
import cn.hutool.db.meta.Table;
import com.haosmart.mysqltoall.config.DbConfig;
import com.haosmart.mysqltoall.ddl.AbstractDdlProvider;
import com.haosmart.mysqltoall.enumration.CaseType;
import com.haosmart.mysqltoall.enumration.CommentType;
import lombok.NonNull;
import org.postgresql.util.PSQLException;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;

public class PostgreSqlDdlProvider extends AbstractDdlProvider {

    public PostgreSqlDdlProvider(DbConfig config, DatabaseMetaData databaseMetaData) throws SQLException {
        super(config, databaseMetaData, CaseType.LOWER);
    }

    @Override
    public CommentType getCommentType() {
        return CommentType.EXTERNAL;
    }

    @Override
    public String buildColumnComment(Column column) {
        if (CharSequenceUtil.isNotBlank(column.getComment())) {
            return CharSequenceUtil.format("COMMENT ON COLUMN {} IS '{}'",
                    this.wrapNameWithSchema(column.getTableName()) + "." + this.wrapName(column.getName()),
                    column.getComment());
        }
        return null;
    }

    @Override
    public String getTableComment(Table table) {
        if (CharSequenceUtil.isNotBlank(table.getComment())) {
            return CharSequenceUtil.format("COMMENT ON TABLE {} IS '{}'",
                    this.wrapNameWithSchema(table.getTableName()),
                    table.getComment());
        }
        return null;
    }

    @Override
    public String handleColumnType(@NonNull Column column) {
        String columnType = column.getTypeName().toUpperCase();
        switch (columnType) {
            case "VARCHAR":
            case "CHAR":
                return columnType + "(" + column.getSize() + ")";
            case "INT":
                return "INTEGER";
            case "DATETIME":
                return "TIMESTAMP";
            case "TINYINT":
                return "SMALLINT";
            case "LONGTEXT":
            case "TEXT":
                return "TEXT";
            case "BLOB":
            case "LONGBLOB":
                return "BYTEA";
            default:
                return columnType;
        }
    }

    @Override
    public String getNameWrapSymbol() {
        return "\"";
    }

    @Override
    public boolean handleThrowable(Throwable e, String tableName) {
        if (e instanceof PSQLException) {
            String sqlState = ((PSQLException) e).getSQLState();
            // 42P07: a relation with the same name already exists
            // 42703: column does not exist
            if ("42P07".equals(sqlState) || "42703".equals(sqlState)) {
                return true;
            }
        }
        return super.handleThrowable(e, tableName);
    }
}
