package com.yxt.data.migration;

import java.sql.SQLException;
import java.util.List;

import com.yxt.data.migration.bean.DataTable;
import com.yxt.data.migration.bean.DataTableStatus;

public interface DbViewer {

	List<DataTable> getTargetTransfterTables() throws SQLException;

	List<String> getTargetTransfterTableColumns(String tableName) throws SQLException;

	List<String> getTargetTransfterTablePrimaryKey(String tableName) throws SQLException;

	List<DataTableStatus> getSourceTransfterTablesStatus() throws SQLException;

	long getSourceTransfterTableMigrationCount(String tableName, String whereClause) throws SQLException;

	long getTargetTransfterTableMigrationFinishedCount(String tableName, String whereClause) throws SQLException;

}