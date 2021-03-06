create or replace procedure DW_DEV_SWIFTGIFT_REPORTING.DBO.Breakage_FIS_ClientInfo_InsertUpdate(RUNDATE varchar)
    returns string not null
    LANGUAGE JAVASCRIPT
    AS
    $$
          
    var result = "";
      try {
		
		var HeaderId = 0; 
        var HeaderIdSql = "SELECT HeaderID FROM DW_DEV_SWIFTGIFT_DWH.DBO.FIS_Import_NonMonetary_Header WHERE date(WorkOfDate) = '" + RUNDATE + "'"
		var HeaderIdSqlStmt = snowflake.createStatement({sqlText: HeaderIdSql});
		var HeaderIdSqlRes = HeaderIdSqlStmt.execute();
		HeaderIdSqlRes.next();
		var HeaderId = HeaderIdSqlRes.getColumnValue(1);
        
        var delete_cmd1 = 'drop table if exists tmpTblFisClientName';
		var sql_delete1 =  snowflake.createStatement({sqlText: delete_cmd1});  
		var delete_result1 = sql_delete1.execute();
        
        var insert_cmd1 = "CREATE TEMPORARY TABLE tmpTblFisClientName AS"
        insert_cmd1 += " SELECT DISTINCT"
        insert_cmd1 += " IssuerClientID,"
        insert_cmd1 += " ClientName,"
        insert_cmd1 += " SubProgramID,"
        insert_cmd1 += " BINCurrencyAlpha"
        insert_cmd1 += " FROM DW_Dev_Swiftgift_DWH.dbo.FIS_Import_NonMonetary_Detail"
        insert_cmd1 += " WHERE HeaderID =" + HeaderId
        var sql_insert1 =  snowflake.createStatement({sqlText: insert_cmd1});  
        var insert_result1 = sql_insert1.execute();
        
        var insert_cmd2 = "INSERT INTO DW_Dev_Swiftgift_DWH.dbo.Breakage_FISClient_Info"
        insert_cmd2 += " (FISClientID,"
        insert_cmd2 += " SubprogramID,"
        insert_cmd2 += " Currency,"
        insert_cmd2 += " FISClientName)"
        insert_cmd2 += " SELECT DISTINCT"
        insert_cmd2 += " IssuerClientID,"
        insert_cmd2 += " SubProgramID,"
        insert_cmd2 += " BINCurrencyAlpha,"
        insert_cmd2 += " ClientName"
        insert_cmd2 += " FROM tmpTblFisClientName"
        insert_cmd2 += " WHERE concat(CAST(IssuerClientID as VARCHAR(10)) ,'-' ,CAST(SubProgramID as VARCHAR(10))) NOT IN ("
        insert_cmd2 += " SELECT DISTINCT"
        insert_cmd2 += " concat(FISClientID , '-' , SubprogramID)"
        insert_cmd2 += " FROM DW_Dev_Swiftgift_DWH.dbo.Breakage_FISClient_Info)"
        var sql_insert2 =  snowflake.createStatement({sqlText: insert_cmd2});  
        var insert_result2 = sql_insert2.execute();
        
        var update_cmd1 = "UPDATE DW_Dev_Swiftgift_DWH.dbo.Breakage_FISClient_Info"
        update_cmd1 += " SET FISClientName = b.ClientName"
        update_cmd1 += " FROM DW_Dev_Swiftgift_DWH.dbo.Breakage_FISClient_Info a INNER JOIN tmpTblFisClientName b ON a.FISClientID = b.IssuerClientID;"
        var sql_update1 =  snowflake.createStatement({sqlText: update_cmd1});  
        var update_result1 = sql_update1.execute(); 

        var delete_cmd2 = 'drop table if exists tmpTblFisClientName';
        var sql_delete2 =  snowflake.createStatement({sqlText: delete_cmd2});  
        var delete_result2 = sql_delete2.execute();
		result = "Succeeded";
        
       }
       catch (err)  {
          result =  "Failed: Code: " + err.code + "\n  State: " + err.state;
          result += "\n  Message: " + err.message;
          result += "\nStack Trace:\n" + err.stackTraceTxt; 
        }
        return result
    $$
    ;