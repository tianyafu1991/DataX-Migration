package com.yxt.data.migration;

import java.sql.SQLException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.yxt.data.migration.bean.DataTable;
import com.yxt.data.migration.bean.DataTableStatus;
import com.yxt.data.migration.util.DataXJobFile;

/**
 * 
 * Generate Datax jobs json files to Datax home/jobs folder
 * @author Jawf Can Li
 * @since 1.0 base on datax 3.0
 * 
 */
@Service
public class JobJsonGenerator {
	protected static final Log log = LogFactory.getLog(JobJsonGenerator.class);

	@Autowired
	private DbViewer viewer;

	@Autowired
	private DataXJobFile jobFile;

	public void generate() throws SQLException {
		//获取源数据库中的表信息
		List<DataTableStatus> sourceTables = viewer.getSourceTransfterTablesStatus();
		//获取目标数据库中的所有表名
		List<DataTable> targetTables = viewer.getTargetTransfterTables();

		if (sourceTables != null) {
			// int i = 0;
			for (DataTable ta : sourceTables) {
				String sourceTableName = ta.getName();
				if (sourceTableName == null || "".equals(sourceTableName)){
					throw new SQLException("Source Table is empty or not existed!");
				}
				//获取目标表的名字，这里只支持原表和目标表的表名一致,如果目标表不存在，则报错
				String targetTableName = getTargetTableName(sourceTableName, targetTables);
				//获取目标数据库中的对应目标表的字段
				List<String> columns = viewer.getTargetTransfterTableColumns(targetTableName);
				//获取到目标数据库中的目标表的主键
				//这里的查询sql应该是需要指定库下面的某个表或者是具体某个schema下面的某个表。否则获取的主键可能是重复的
				List<String> pks = viewer.getTargetTransfterTablePrimaryKey(targetTableName);
				//获取where条件
				String whereClause = jobFile.getSourceGlobalTableWhereClause(columns);
				//获取源数据库中一个具体的表的符合where条件的数据个数有多少
				long migrationRecords = viewer.getSourceTransfterTableMigrationCount(sourceTableName, whereClause);
				String pk = null;
				if (pks != null && !pks.isEmpty()) {
					if (pks.size() == 1) {
						pk = pks.get(0);
					}
				}
				//生成json文件,重要步骤
				jobFile.generateJsonJobFile(sourceTableName, targetTableName, columns, pk, whereClause, migrationRecords);
				// i++;
				// if (i==30)
				// break;// remove this line*/
			}
		}
	}

	private String getTargetTableName(String sourceTableName, List<DataTable> targetTables) throws SQLException {
		String result = null; 
		if (sourceTableName!=null && targetTables!=null){
			for (DataTable t:targetTables){
				if (sourceTableName.equalsIgnoreCase(t.getName())){
					result = t.getName();
					break;
				}
			}
		}
		if (result == null){
			String errorMsg = "Target Table for "+sourceTableName+" is empty or not existed!";
			log.error(errorMsg);
			throw new SQLException(errorMsg);
		}
		return result;
	}

}
