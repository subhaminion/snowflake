USE [Dev_Swiftgift_DWH]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[Reports_Breakage_All_Client_V2]
    @ReportRunDate DATE = NULL
,   @SendEmailTo NVARCHAR(MAX) = NULL
AS
BEGIN 
    SET NOCOUNT ON;
    DECLARE @CardSweepDate DATE
    ,   @MonthYear AS INT,
	@EmailReplacementKeyVals NVARCHAR(max)
  	  
    IF @ReportRunDate IS NULL
        SET @ReportRunDate = GETDATE();

    SET @CardSweepDate = CAST(MONTH(@ReportRunDate) AS NVARCHAR(10)) + '/3/' + CAST(YEAR(@ReportRunDate) AS NVARCHAR(10));

    DECLARE @CardExpirationLookup_StartDate DATE
    ,   @CardExpirationLookup_EndDate DATETIME;
    SET @CardExpirationLookup_StartDate = DATEADD(MONTH, DATEDIFF(MONTH, 0, @ReportRunDate) - 2, 0);
    SET @CardExpirationLookup_EndDate = CAST(CAST(EOMONTH(@CardExpirationLookup_StartDate) AS NVARCHAR(50)) + ' 23:59:59' AS DATETIME);  
	 
    SET @MonthYear = CAST(CAST(MONTH(@CardExpirationLookup_EndDate) AS NVARCHAR(5)) + CAST(YEAR(@CardExpirationLookup_EndDate) AS NVARCHAR(5)) AS INT);  
    
    BEGIN -- Step 1 : Fetch Data 
        DECLARE @LoopDSPGClientID BIGINT
        ,   @LoopDSPGSubProgramID BIGINT
        ,   @csvClientIdsToFetchDataFor NVARCHAR(MAX)
        ,@Environment NVARCHAR(MAX) = dbo.fn_ENVIRONMENT_CONFIGURATION('ENVIRONMENT')
        
        IF CURSOR_STATUS('global', 'cr_CRM_DSPG') >= -1
            BEGIN
                CLOSE cr_CRM_DSPG;
                DEALLOCATE cr_CRM_DSPG;
            END;
        DECLARE cr_CRM_DSPG CURSOR FAST_FORWARD READ_ONLY
        FOR
        SELECT DISTINCT
                CAST([FISClientID] AS BIGINT), CAST([SubprogramID] AS BIGINT)
       -- FROM    Dev_Swiftgift_CRM.dbo.CRM_FIS_DetailRecord_DSPG WITH ( NOLOCK );  -- as per lync conversation with ruth and Tracy on 10092015we need to populate it from the same table as we fetch curency from 
        FROM    dbo.Breakage_FISClient_Info WITH ( NOLOCK );
		

        OPEN cr_CRM_DSPG;
        FETCH NEXT FROM cr_CRM_DSPG INTO @LoopDSPGClientID, @LoopDSPGSubProgramID;
        WHILE @@FETCH_STATUS = 0
            BEGIN
                
                IF NOT EXISTS ( SELECT  '*'
                                FROM    dbo.Breakage_Data WITH ( NOLOCK )
                                WHERE   MonthYear = @MonthYear
                                        AND FISClientID = @LoopDSPGClientID
                                        AND FISSubProgramID = @LoopDSPGSubProgramID )
                    BEGIN 
                        BEGIN TRY 
                            EXEC dbo.Reports_Breakage_Retrieve_Data_V2 @CSVFISClientIds = @LoopDSPGClientID, -- nvarchar(max)
                                @ReportRunDate = @ReportRunDate; -- date  -- We are not passing the subprogram as when this query runs it populates for all sub program ids and we do not need to rerun 
                               
								 
                            IF NOT EXISTS ( SELECT  '*'
                                            FROM    dbo.Breakage_Data WITH ( NOLOCK )
                                            WHERE   MonthYear = @MonthYear
                                                    AND FISClientID = @LoopDSPGClientID
                                                    AND FISSubProgramID = @LoopDSPGSubProgramID )
                                BEGIN
                                    INSERT  INTO dbo.Breakage_Data ( FISClientID, FISSubProgramID, MonthYear, FISClientName, FISSubProgramName, TotalCard,
                                                                     TotalLoad, Breakage, InsertDate )
                                            SELECT  [FISClientID], [SubprogramID], @MonthYear, MAX([Client Name]), MAX([Sub Program Name]), 0, 0, 0, GETDATE()
                                            FROM    dbo.Breakage_FISClient_Info a WITH ( NOLOCK )
                                            LEFT JOIN Dev_Swiftgift_CRM.dbo.CRM_FIS_DetailRecord_DSPG b WITH ( NOLOCK ) ON a.FISClientID = b.[Client ID]
                                                                                                                           AND a.SubprogramID = b.[Sub Program ID]
                                            WHERE   [SubprogramID] = @LoopDSPGSubProgramID
                                                    AND [FISClientID] = @LoopDSPGClientID
                                            GROUP BY [FISClientID], [SubprogramID];
                                END; 
                        END TRY 
                        BEGIN CATCH
                        END CATCH;
						
                    END; 

					 
                FETCH NEXT FROM cr_CRM_DSPG INTO @LoopDSPGClientID, @LoopDSPGSubProgramID;
            END;
        CLOSE cr_CRM_DSPG;
        DEALLOCATE cr_CRM_DSPG;
        --PRINT @csvClientIdsToFetchDataFor;
		
       
		
    END; 
    PRINT 'data population done';
    BEGIN -- Step 2: All Fis Client Id's Tab 
        IF OBJECT_ID('tempdb..##tmpAllFisClientId') IS NOT NULL
            BEGIN 
                DROP TABLE  ##tmpAllFisClientId;
            END;
		
        WITH    cteAllBreakageData ( FISClientID, FISSubProgramID, AllBreakageLoad, AllBreakageCards, AllBreakageValue )
                  AS ( SELECT   FISClientID, FISSubProgramID, SUM(TotalLoad) AS [AllBreakageLoad], SUM(TotalCard) AS [AllBreakageCards],
                                SUM(Breakage) AS [AllBreakageValue]
                       FROM     dbo.Breakage_Data WITH ( NOLOCK )
                       WHERE    CAST(RIGHT(CAST(MonthYear AS NVARCHAR(50)), 4) AS INT) <= YEAR(@CardExpirationLookup_StartDate)
                                AND CAST(REPLACE(CAST(MonthYear AS NVARCHAR(50)), RIGHT(CAST(MonthYear AS NVARCHAR(50)), 4), '') AS INT) <= MONTH(@CardExpirationLookup_StartDate)
                       GROUP BY FISClientID, FISSubProgramID)
            SELECT  IDENTITY ( INT, 1, 1 ) AS RowID, a.[FIS Client ID], a.[FIS Client Name], a.[FIS Subprogram ID], a.[FIS Subprogram Name], a.Currency,
                    a.[Total Cards], a.[Total Load], a.[Avg Load], a.[Net Breakage], a.[Breakage %], a.[Total Cards to Date], a.[Total Load to Date],
                    a.[Avg Load to Date], a.[Total Breakage to Date], a.[Breakage % to Date]
            INTO    ##tmpAllFisClientId
            FROM    ( SELECT    CAST(a.FISClientID AS NVARCHAR(50)) AS [FIS Client ID], UPPER(a.FISClientName) AS [FIS Client Name],
                                CAST(a.FISSubProgramID AS NVARCHAR(50)) AS [FIS Subprogram ID], UPPER(FISSubProgramName) AS [FIS Subprogram Name],
                                Currency AS [Currency], TotalCard AS [Total Cards], TotalLoad AS [Total Load], TotalLoad / NULLIF(TotalCard, 0) AS [Avg Load],
                                Breakage AS [Net Breakage], CAST(( [Breakage] / NULLIF(TotalLoad, 0) ) * 100 AS DECIMAL(8, 2)) AS [Breakage %],
                                allBreakage.AllBreakageCards AS [Total Cards to Date], allBreakage.AllBreakageLoad AS [Total Load to Date],
                                AllBreakageLoad / NULLIF(AllBreakageCards, 0) AS [Avg Load to Date], AllBreakageValue AS [Total Breakage to Date],
                                CAST(( AllBreakageValue / NULLIF(AllBreakageLoad, 0) ) AS DECIMAL(8, 2)) * 100 AS [Breakage % to Date]
                      FROM      dbo.Breakage_Data a WITH ( NOLOCK )
                      INNER JOIN dbo.Breakage_FISClient_Info fs WITH ( NOLOCK ) ON fs.FISClientID = a.FISClientID
                                                                                   AND fs.SubprogramID = a.FISSubProgramID
                      INNER JOIN cteAllBreakageData allBreakage ON allBreakage.FISClientID = a.FISClientID
                                                                   AND allBreakage.FISSubProgramID = a.FISSubProgramID
                      WHERE     MonthYear = @MonthYear
                      UNION ALL
                      SELECT    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL ) a
            ORDER BY a.[FIS Client Name];
      
        DELETE  FROM ##tmpAllFisClientId
        WHERE   [FIS Client ID] IS NULL; 
		
        INSERT  INTO ##tmpAllFisClientId ( [FIS Client ID] )
        VALUES  ( NULL );
			
        INSERT  INTO ##tmpAllFisClientId ( [FIS Client ID], [FIS Client Name], [FIS Subprogram ID], [FIS Subprogram Name], Currency, [Total Cards], [Total Load],
                                           [Avg Load], [Net Breakage], [Breakage %], [Total Cards to Date], [Total Load to Date], [Avg Load to Date],
                                           [Total Breakage to Date], [Breakage % to Date] )
                SELECT  'Total', NULL, NULL, NULL, Currency, SUM([Total Cards]), SUM([Total Load]), SUM([Avg Load]), SUM([Net Breakage]), NULL,
                        SUM([Total Cards to Date]), SUM([Total Load to Date]), SUM([Avg Load to Date]), SUM([Total Breakage to Date]), NULL
                FROM    ##tmpAllFisClientId
                WHERE   [FIS Client ID] IS NOT NULL
                GROUP BY Currency;


    END; 



    BEGIN -- Step 3: Client Breakage Tab
        IF OBJECT_ID('tempdb..##tmpBreakageSummary') IS NOT NULL
            BEGIN 
                DROP TABLE  ##tmpBreakageSummary;
            END;
        DECLARE @BrSummary_BreakageID NVARCHAR(50);
			
        DECLARE cr_BSSummary CURSOR FAST_FORWARD READ_ONLY
        FOR
        SELECT  BreakageID
        FROM    SWIFT_DB.Dev_Swiftgift_2.dbo.SG_BreakageReport br WITH ( NOLOCK )
        WHERE   br.Active = 1
                AND br.Deleted = 0;
        OPEN cr_BSSummary;
        FETCH NEXT FROM cr_BSSummary INTO @BrSummary_BreakageID;
        WHILE @@FETCH_STATUS = 0
            BEGIN
                IF NOT EXISTS ( SELECT  '*'
                                FROM    dbo.BreakageReport_Summary WITH ( NOLOCK )
                                WHERE   BreakageID = @BrSummary_BreakageID
                                        AND MonthYear = @MonthYear )
                    BEGIN 
                        EXEC dbo.Reports_Breakage_Client_Facing_V2 @BreakageID = @BrSummary_BreakageID, -- nvarchar(50)
                            @ReportRunDate = @ReportRunDate, -- date		
                            @SendEmailTo = 'NoOne';
                    END; 
                FETCH NEXT FROM cr_BSSummary INTO @BrSummary_BreakageID;
            END;
        CLOSE cr_BSSummary;
        DEALLOCATE cr_BSSummary;
        SELECT  IDENTITY ( INT, 1, 1 ) AS RowID, a.[OMSI Client ID], a.[OMSI Client Name], a.[Breakage Report Name], a.[FIS Client ID], a.[FIS Client Name],
                a.Currency, a.[Total Cards], a.[Total Load], a.[Avg Load], a.[Total Breakage], a.[Breakage %], a.[Vat Tax], a.[Shared Reissue Count],
                a.[Shared Reissue Total], a.[Net Swift Breakage], a.[Client Share Percentage], a.[Card Fee Deduction], a.[Unfunded Card Fee Deduction],
                a.[Client Reissue Count], a.[Client Reissue Total], a.[Additional Deductions], a.[Current Month Breakage], a.[Previous Month Negative],
                a.[Breakage Owed], a.BreakageID
        INTO    ##tmpBreakageSummary
        FROM    ( SELECT    cl.Client_ID AS [OMSI Client ID], UPPER(cl.CompanyName) AS [OMSI Client Name],
                            UPPER(br.BreakageReportName) AS [Breakage Report Name], FISClientIDs AS [FIS Client ID], UPPER(FISClientNames) AS [FIS Client Name],
                            Currency, TotalCards AS [Total Cards], TotalLoad AS [Total Load], CASE WHEN TotalCards = 0 THEN 0
                                                                                                   ELSE TotalLoad / TotalCards
                                                                                              END AS [Avg Load], TotalBreakage AS [Total Breakage],
                            CASE WHEN TotalLoad = 0 THEN 0
                                 ELSE CAST(( CAST(TotalBreakage AS MONEY) / CAST(TotalLoad AS MONEY) ) * 100 AS DECIMAL(8, 2))
                            END AS [Breakage %], bs.SharedReissueCount AS [Shared Reissue Count], SharedReissueTotal AS [Shared Reissue Total],
                            CAST(br.ClientShare AS NVARCHAR(50)) + ' %' AS [Client Share Percentage], CardFeeDeductiion AS [Card Fee Deduction],
                            UnFundedCardFeeDeduction AS [Unfunded Card Fee Deduction], bs.ClientReissueCount AS [Client Reissue Count],
                            ClientReissueTotal AS [Client Reissue Total], AdditionalAdjustments AS [Additional Deductions],
                            CASE WHEN bs.Currency = 'CAD' THEN bs.TotalBreakage * 0.12
                                 ELSE 0
                            END AS [Vat Tax], ( TotalBreakage - SharedReissueTotal ) * ( 1 - ( ClientShare / 100 ) ) AS [Net Swift Breakage],
                            CAST(NULL AS MONEY) AS [Previous Month Negative], BreakageOwed AS [Breakage Owed], br.BreakageID,
                            bs.CurrentMonthBreakage AS [Current Month Breakage]
                  FROM      dbo.BreakageReport_Summary bs WITH ( NOLOCK )
                  INNER JOIN SWIFT_DB.Dev_Swiftgift_2.dbo.SG_BreakageReport br WITH ( NOLOCK ) ON br.BreakageID = bs.BreakageID
                  INNER JOIN SWIFT_DB.Dev_Swiftgift_2.dbo.SG_Clients cl WITH ( NOLOCK ) ON br.ClientID = cl.Client_ID
                  WHERE     bs.MonthYear = @MonthYear
                            AND br.Active = 1
                            AND br.Deleted = 0
                  UNION ALL
                  SELECT    NULL AS [OMSI Client ID], NULL AS [OMSI Client Name], NULL AS [Breakage Report Name], NULL AS [FIS Client ID],
                            NULL AS [FIS Client Name], NULL AS Currency, NULL AS [Total Cards], NULL AS [Total Load], NULL AS [Avg Load],
                            NULL AS [Total Breakage], NULL AS [Breakage %], NULL AS [Shared Reissue Count], NULL AS [Shared Reissue Total],
                            NULL AS [Client Share Percentage], NULL AS [Card Fee Deduction], NULL AS [Unfunded Card Fee Deduction],
                            NULL AS [Client Reissue Count], NULL AS [Client Reissue Total], NULL AS [Additional Deductions], NULL AS [Vat Tax],
                            NULL AS [Net Swift Breakage], NULL AS [Previous month Negative], NULL AS [Breakage Owed], NULL AS BreakageID,
                            NULL AS [Current Month Breakage] ) a; 
        DELETE  FROM ##tmpBreakageSummary
        WHERE   [OMSI Client ID] IS NULL;
		
        INSERT  INTO ##tmpBreakageSummary ( [OMSI Client ID] )
        VALUES  ( NULL ); 

		
        WITH    CTE_Ledger
                  AS ( SELECT   BreakageID, leg.OutstandingBalance,
                                ROW_NUMBER() OVER ( PARTITION BY leg.BreakageID ORDER BY leg.TransactionDate DESC, leg.TransactionID DESC ) AS rnk
                       FROM     SWIFT_DB.Dev_Swiftgift_2.dbo.SG_Breakage_Ledger leg WITH ( NOLOCK )
                       WHERE    leg.TransactionDate <= @CardExpirationLookup_StartDate
                                AND TransBy = 'Auto')
            UPDATE  ##tmpBreakageSummary
            SET     [Previous Month Negative] = CASE WHEN b.OutstandingBalance > 0 THEN -1
                                                     ELSE 1
                                                END * b.OutstandingBalance
            FROM    ##tmpBreakageSummary a
            INNER JOIN CTE_Ledger b ON b.BreakageID = a.BreakageID
            WHERE   b.rnk = 1; 

  
        INSERT  INTO ##tmpBreakageSummary ( [OMSI Client ID], [OMSI Client Name], [Breakage Report Name], [FIS Client ID], [FIS Client Name], Currency,
                                            [Total Cards], [Total Load], [Avg Load], [Total Breakage], [Breakage %], [Shared Reissue Count],
                                            [Shared Reissue Total], [Client Share Percentage], [Card Fee Deduction], [Unfunded Card Fee Deduction],
                                            [Client Reissue Count], [Client Reissue Total], [Additional Deductions], [Vat Tax], [Net Swift Breakage],
                                            [Previous Month Negative], [Breakage Owed], BreakageID, [Current Month Breakage] )
                SELECT  'Total', NULL, NULL, NULL, NULL, Currency, SUM([Total Cards]), SUM([Total Load]), NULL, SUM([Total Breakage]), NULL,
                        SUM([Shared Reissue Count]), SUM([Shared Reissue Total]), NULL, SUM([Card Fee Deduction]), SUM([Unfunded Card Fee Deduction]),
                        SUM([Client Reissue Count]), SUM([Client Reissue Total]), SUM([Additional Deductions]), SUM([Vat Tax]), SUM([Net Swift Breakage]),
                        SUM([Previous Month Negative]), SUM([Breakage Owed]), NULL, SUM([Current Month Breakage])
                FROM    ##tmpBreakageSummary
                WHERE   [OMSI Client ID] IS NOT NULL
                GROUP BY Currency;
			

    END;
    BEGIN -- Step 4: Payments Tab
        IF OBJECT_ID('tempdb..##tmpBreakagePayments') IS NOT NULL
            BEGIN 
                DROP TABLE  ##tmpBreakagePayments;
            END;
        SELECT  cl.Client_ID AS [OMSI Client ID], UPPER(cl.CompanyName) AS [OMSI Client Name], UPPER(br.BreakageReportName) AS [Breakage Report Name], Currency,
                BreakageOwed AS [Breakage Owed], UPPER(Remittance) AS Remittance, br.BreakageID, br.BreakageReportName, br.BreakageGPID, bs.CurrentMonthBreakage
        INTO    ##tmpBreakagePayments
        FROM    dbo.BreakageReport_Summary bs WITH ( NOLOCK )
        INNER JOIN SWIFT_DB.Dev_Swiftgift_2.dbo.SG_BreakageReport br WITH ( NOLOCK ) ON br.BreakageID = bs.BreakageID
        INNER JOIN SWIFT_DB.Dev_Swiftgift_2.dbo.SG_Clients cl WITH ( NOLOCK ) ON br.ClientID = cl.Client_ID
        WHERE   bs.MonthYear = @MonthYear
                AND br.Active = 1
                AND br.Deleted = 0; 
    END; 

    BEGIN -- Step 5 - Breakage GP Output 
        IF OBJECT_ID('tempdb..##tmpBreakageGPOutput') IS NOT NULL
            BEGIN 
                DROP TABLE  ##tmpBreakageGPOutput;
            END;
			
        WITH    CTE_GPOutput
                  AS ( SELECT   CASE WHEN [CurrentMonthBreakage] > 0 THEN 'INVOICE'
                                     ELSE 'CREDIT MEMO'
                                END AS [TYPE],
                                REPLACE(CONVERT(NVARCHAR, @CardExpirationLookup_EndDate, 101), '/', '') + ' ' + [OMSI Client ID] + ' ' + Currency AS [DOCNUMBER],
                                REPLACE(CONVERT(NVARCHAR, @CardExpirationLookup_EndDate, 101), '/', '') AS [DOC. DATE],
                                REPLACE(CONVERT(NVARCHAR, @CardExpirationLookup_EndDate, 101), '/', '') + ' ' + [OMSI Client ID] + ' ' + Currency AS [DESCRIPTION],
                                UPPER(BreakageGPID) AS [VENDOR ID], 'BEST WAY' AS [SHIPPING METHOD], CASE WHEN [CurrentMonthBreakage] < 0 THEN -1
                                                                                                          ELSE 1
                                                                                                     END * [CurrentMonthBreakage] AS [PURCHASES],
                                CASE WHEN Currency = 'USD' THEN 'Z-US$'
                                     WHEN Currency = 'CAD' THEN 'Z-C$'
                                     WHEN Currency = 'GBP' THEN 'Z-UK'
                                     WHEN Currency = 'EUR' THEN 'Z-EURO'
                                     WHEN Currency = 'AUD' THEN 'Z-AUD'
                                     WHEN Currency = 'HKD' THEN 'Z-HKD'
                                     WHEN Currency = 'JPY' THEN 'Z-JPY'
                                     WHEN Currency = 'ZAR' THEN 'Z-SA'
                                     WHEN Currency = 'INR' THEN 'Z-INR'
                                     ELSE ''
                                END AS [CURRENCY]
                       FROM     ##tmpBreakagePayments
                       WHERE    [CurrentMonthBreakage] <> 0)
            SELECT  a.TYPE, a.DOCNUMBER, MAX(a.[DOC. DATE]) AS [DOC. DATE], a.DESCRIPTION, a.[VENDOR ID], MAX(a.[SHIPPING METHOD]) AS [SHIPPING METHOD],
                    SUM(a.PURCHASES) AS PURCHASES, a.CURRENCY
            INTO    ##tmpBreakageGPOutput
            FROM    CTE_GPOutput a
            GROUP BY a.TYPE, a.DOCNUMBER, a.DESCRIPTION, a.[VENDOR ID], a.CURRENCY;
	  

        DECLARE @ExportedInvoiceFileName NVARCHAR(MAX)
        
        IF @Environment <> 'PRODUCTION'
            BEGIN   
                SET @ExportedInvoiceFileName = 'C:\TEMP\SwiftGift\BreakageReport\GPExport\'; 
            END;   
        ELSE
            BEGIN   
                SET @ExportedInvoiceFileName = 'S:\SqlExportedFiles\BreakageReport\GPExport\';  
            END;   

        SET @ExportedInvoiceFileName = @ExportedInvoiceFileName + 'GP_Breakage' + CASE WHEN @Environment = 'PRODUCTION' THEN '.csv'
                                                                                       ELSE '_Test.csv'
                                                                                  END; 

        EXECUTE dbo.Shared_ExportQueryToCSV @DBFetch = '##tmpBreakageGPOutput', @PCWrite = @ExportedInvoiceFileName;  

        
						
						SET @SendEmailTo  = ISNULL(@SendEmailTo,'OMSIReports@swiftprepaid.com;AUTOQA@DAVINCIPAY.COM')
						SET @EmailReplacementKeyVals = '#date#:' + DATENAME(MONTH, @CardExpirationLookup_EndDate)
                                             + CAST(YEAR(@CardExpirationLookup_EndDate) AS NVARCHAR(50))
						EXEC Dev_Swiftgift_DWH_EXT.dbo.Shared_send_dbmail @Id = N'REPORTS_GP_BREAKAGE',                      -- nvarchar(256)
						                                                  @EmailRecipients = @SendEmailTo,         -- nvarchar(max)
						                                                  @EmailReplacementKeyVals = @EmailReplacementKeyVals, -- nvarchar(max)
						                                                  @file_attachments = @ExportedInvoiceFileName
						
  
         
        DECLARE @FTPCommand_GPExport VARCHAR(4000) = CASE WHEN @Environment = 'PRODUCTION'
                                                          THEN 'S:\SFTPScripts\SFTP.Bat "' + @ExportedInvoiceFileName + '" ' + ' "/FTP/OMSIFTP/ExportedFiles/Liventus/BreakageReports/'
                                                               + DATENAME(MONTH, @CardExpirationLookup_StartDate) + DATENAME(YEAR,
                                                                                                                             @CardExpirationLookup_StartDate)
                                                               + '"'
                                                          ELSE ''
                                                     END;
        --PRINT @FTPCommand_GPExport;
        EXEC xp_cmdshell @FTPCommand_GPExport;
        IF OBJECT_ID('tempdb..##tmpBreakageGPOutput') IS NOT NULL
            BEGIN 
                DROP TABLE  ##tmpBreakageGPOutput;
            END;

    END; 

    ALTER TABLE ##tmpBreakagePayments DROP COLUMN BreakageID;
    ALTER TABLE ##tmpBreakagePayments DROP COLUMN BreakageReportName;
    ALTER TABLE ##tmpBreakagePayments DROP COLUMN BreakageGPID;
    ALTER TABLE ##tmpBreakagePayments DROP COLUMN CurrentMonthBreakage;


    DELETE  FROM ##tmpBreakagePayments
    WHERE   [Breakage Owed] <= 0;

    DECLARE @ExcelFilename NVARCHAR(4000)
    ,   @ExcelFileCreated BIT
    ,   @ExcelTemplatePath NVARCHAR(MAX)
    ,   @csvworksheetsname NVARCHAR(256)
    ,   @worksheet1name NVARCHAR(32)
    ,   @worksheet2name NVARCHAR(32)
          
                  
    SET @csvworksheetsname = @worksheet1name + ',' + @worksheet2name;
    SET @ExcelFilename = CASE WHEN @Environment = 'PRODUCTION' THEN 'S:\SqlExportedFiles\BreakageReport\'
                              ELSE 'C:\TEMP\SwiftGift\BreakageReport\'
                         END + 'All Client Breakage Summary_' + CAST(YEAR(@CardExpirationLookup_EndDate) AS NVARCHAR(20)) + ' ' + DATENAME(MONTH,
                                                                                                                                           @CardExpirationLookup_EndDate)
        + '_' + REPLACE(CONVERT(NVARCHAR, @CardSweepDate, 111), '/', '') + '.xlsx';

    DECLARE @isFileExists INT;
    EXEC master.dbo.xp_fileexist @ExcelFilename, @isFileExists OUTPUT;

	
    IF @isFileExists = 1
        BEGIN 
            DECLARE @DeleteFileQuery VARCHAR(4000);
            SET @DeleteFileQuery = 'del "' + @ExcelFilename + '"';
            EXEC xp_cmdshell @DeleteFileQuery;
        END; 

    SET @EmailReplacementKeyVals = '#date#:' + CAST(YEAR(@CardExpirationLookup_EndDate) AS NVARCHAR(20)) + ' ' + DATENAME(MONTH,
                                                                                                                                    @CardExpirationLookup_EndDate)
        + '_' + REPLACE(CONVERT(NVARCHAR, @CardSweepDate, 111), '/', ''); 

    DECLARE @csvWorkSheetName NVARCHAR(MAX) = ''
    ,   @csvTempTableName NVARCHAR(MAX) = ''
    ,   @csvCurrencyTablename NVARCHAR(MAX)= ''
    ,   @csvCurrencySheetName NVARCHAR(MAX)= ''
    ,   @csvDoNotIncludeColmuns NVARCHAR(MAX) = ''
    ,   @indvCurrencyTempTableName NVARCHAR(100)
    ,   @DynamicQuery NVARCHAR(MAX);

    IF OBJECT_ID('tempdb..#tmpBreakageDataDetails') IS NOT NULL
        BEGIN 
            DROP TABLE  #tmpBreakageDataDetails;
        END;

    IF OBJECT_ID('tempdb..#tmpNonMon') IS NOT NULL
        BEGIN 
            DROP TABLE  #tmpNonMon;
        END;

    SELECT  *, CAST(NULL AS NVARCHAR(50)) AS [Currency], CAST(NULL AS NVARCHAR(500)) AS [OMSIProgram], CAST(NULL AS NVARCHAR(500)) AS [OMSILocation],
            CAST(NULL AS NVARCHAR(500)) AS [RegFirstName], CAST(NULL AS NVARCHAR(500)) AS [RegLastName], CAST(NULL AS NVARCHAR(500)) AS [RegAddr1],
            CAST(NULL AS NVARCHAR(500)) AS [RegAddr2], CAST(NULL AS NVARCHAR(500)) AS [RegCity], CAST(NULL AS NVARCHAR(500)) AS [RegState],
            CAST(NULL AS NVARCHAR(500)) AS [RegZip], CAST(NULL AS NVARCHAR(500)) AS [RegCountry], 
			 SUBSTRING(
                        ResidentialAddress3,
                        0,
                        IIF(CHARINDEX('_', ResidentialAddress3) > 0,
                            CHARINDEX('_', ResidentialAddress3),
                            LEN(ResidentialAddress3) + 1)
                    ) AS Omsi_OrderId,
		    CAST(NULL AS NVARCHAR(50)) AS CurrencyPageName
    INTO    #tmpBreakageDataDetails
    FROM    dbo.Breakage_Data_Details WITH ( NOLOCK )
    WHERE   MonthYear = @MonthYear
	
    UPDATE  #tmpBreakageDataDetails
    SET     Currency = b.Currency
    FROM    #tmpBreakageDataDetails a
    INNER JOIN Breakage_FISClient_Info b WITH ( NOLOCK ) ON b.FISClientID = a.FISClientID;

	CREATE NONCLUSTERED INDEX ix_tempBreakageCardNumberProxy ON #tmpBreakageDataDetails (CardNumberProxy);

	-- Fetch the latest profile on the card --
	SELECT *
	INTO #tmpNonMon FROM (
	SELECT  nmd.FISProxyNumber as CardNumberProxy, nmd.CardholderFirstName, nmd.CardholderLastName, nmd.MailingAddressLine1, nmd.MailingAddressLine2,
                nmd.MailingCity, nmd.MailingState, nmd.MailingZip, nmd.CardholderCountryCode,
                DENSE_RANK() OVER ( PARTITION BY nmd.FISProxyNumber ORDER BY nmd.[ID] DESC ) AS rnk
        FROM  dbo.FIS_Import_NonMonetary_Detail nmd WITH ( NOLOCK , INDEX = IX_FIS_NonMonetary_Detail_Proxy_V2) 
		WHERE EXISTS (
			SELECT 1 FROM #tmpBreakageDataDetails a WHERE a.CardNumberProxy = nmd.FISProxyNumber		   
			)
		) AS x WHERE rnk=1;
	
    UPDATE  #tmpBreakageDataDetails
    SET     RegFirstName = ct.CardholderFirstName, RegLastName = ct.CardholderLastName, RegAddr1 = ct.MailingAddressLine1, RegAddr2 = ct.MailingAddressLine2,
            RegCity = ct.MailingCity, RegState = ct.MailingState, RegZip = ct.MailingZip, RegCountry = ct.CardholderCountryCode
    FROM    #tmpBreakageDataDetails tmp
    INNER JOIN #tmpNonMon ct ON ct.CardNumberProxy = tmp.CardNumberProxy;


	-- Fetch the latest profile on the card --

    --UPDATE  #tmpBreakageDataDetails
    --SET     OMSIProgram = UPPER(prg.ProgramName), OMSILocation = UPPER(loc.Location_ID + ' - ' + loc.LocationName)
    --FROM    #tmpBreakageDataDetails a
    --LEFT JOIN SWIFT_DB.Dev_Swiftgift_2.dbo.SG_Orders ord WITH ( NOLOCK ) ON a.ResidentialAddress3 = ord.Order_ID
    --LEFT JOIN SWIFT_DB.Dev_Swiftgift_2.dbo.SG_BatchOrderUpload bord WITH ( NOLOCK ) ON a.ResidentialAddress3 = bord.Order_id
    --LEFT JOIN SWIFT_DB.Dev_Swiftgift_2.dbo.SG_FundingRequest_Cards fc WITH ( NOLOCK ) ON fc.FundingID = a.ClientRefNum
    --LEFT JOIN SWIFT_DB.Dev_Swiftgift_2.dbo.SG_Programs prg WITH ( NOLOCK ) ON prg.Program_ID = ord.Program_ID
    --                                                                          OR prg.Program_ID = bord.Program_ID
    --                                                                          OR prg.Program_ID = fc.Program_ID
    --LEFT JOIN SWIFT_DB.Dev_Swiftgift_2.dbo.SG_Locations loc WITH ( NOLOCK ) ON loc.Row_loc_ID = ord.Row_loc_ID
    --                                                                           OR loc.Row_loc_ID = bord.Row_Loc_id
    --                                                                           OR loc.Row_loc_ID = fc.Row_Loc_ID;



	--split currency part if row count limit exceeded
	DECLARE @CurrencyPageSize INT = 250000;
	UPDATE #tmpBreakageDataDetails
	SET CurrencyPageName = (CASE WHEN b.PageNum<>1 then Currency+'_'+CAST(b.PageNum AS NVARCHAR(30)) ELSE Currency END)
	FROM #tmpBreakageDataDetails 
	INNER JOIN
	(
		SELECT RowId,((ROW_NUMBER() OVER (PARTITION BY Currency ORDER BY RowId)+@CurrencyPageSize-1)/@CurrencyPageSize) PageNum
		FROM #tmpBreakageDataDetails
	) AS b
	ON #tmpBreakageDataDetails.RowId=b.RowId;



		 /* declare variables */
    DECLARE @Currency NVARCHAR(50);
		 
    DECLARE cur_Currency CURSOR FAST_FORWARD READ_ONLY
    FOR
    SELECT DISTINCT
            CurrencyPageName
    FROM    #tmpBreakageDataDetails ORDER BY 1;
		 
    OPEN cur_Currency;
		 
    FETCH NEXT FROM cur_Currency INTO @Currency;
		 
    WHILE @@FETCH_STATUS = 0
        BEGIN
            SET @indvCurrencyTempTableName = '##tmpBreakageAllClientDetails_' + ISNULL(@Currency, 'null');
            EXEC(' IF OBJECT_ID(''tempdb..' + @indvCurrencyTempTableName + ''') IS NOT NULL drop table ' + @indvCurrencyTempTableName);
			
            SET @DynamicQuery = 'SELECT  a.Rowid, a.FISClientID, a.FISClientName, a.AcctEnding, a.CardNumberProxy as [Proxy],  Replace(CONVERT(NVARCHAR(20), a.CreateDate, 102),''.'',''/'') + '' ET'' AS [Date Created], 
			Currency as [Currency], 
			 a.ValueLoad as [Load], a.Breakage,  Replace(CONVERT(NVARCHAR(20), ExpirationDate, 102),''.'',''/'') + '' ET'' AS [Expiration] 
            , a.Omsi_OrderId as [OMSI Order ID], a.OMSIProgram as [OMSI Program], a.OMSILocation as [OMSI Location], 
			a.CardHolderFirstName  AS [First Name], a.CardHolderLastName AS [Last Name],  MailingAddressLine1 as [Original Address 1],
            MailingAddressLine2 as [Original Address 2], MailingCity as [Original City], MailingState as [Original State], MailingZip as [Original ZipCode], CardHolderCountryCode as [Original Country],
			a.CardHolderSSN AS [Client Key], a.ClientUniqueID as [Client Unique ID],  a.Comment as [Comment],
            a.PaymentRefNo, [RegFirstName] AS [Registered FirstName], [RegLastName] AS [Registered LastName], [RegAddr1] AS [Registered Addr1],
            [RegAddr2] AS [Registered Addr2], [RegCity] AS [Registered City], [RegState] AS [Registered State],
            [RegZip] AS [Registered Zip], [RegCountry] AS [Registered Country]
			into ' + @indvCurrencyTempTableName + '
			FROM    #tmpBreakageDataDetails a 
			  WHERE   CurrencyPageName = ''' + @Currency + '''';  
			  /*
            FROM    #tmpBreakageDataDetails a 
            INNER JOIN dbo.Breakage_FISClient_Info ci WITH ( NOLOCK ) ON ci.FISClientID = a.FISClientID
            LEFT JOIN SWIFT_DB.Dev_Swiftgift_2.dbo.SG_Orders ord WITH ( NOLOCK ) ON a.ResidentialAddress3 = ord.Order_ID
            LEFT JOIN SWIFT_DB.Dev_Swiftgift_2.dbo.SG_BatchOrderUpload bord WITH ( NOLOCK ) ON a.ResidentialAddress3 = bord.Order_ID
            LEFT JOIN SWIFT_DB.Dev_Swiftgift_2.dbo.SG_Programs prg WITH ( NOLOCK ) ON prg.Program_ID = ord.Program_ID or prg.Program_ID = bord.Program_ID 
            LEFT JOIN SWIFT_DB.Dev_Swiftgift_2.dbo.SG_Locations loc WITH ( NOLOCK ) ON loc.Row_loc_ID = ord.Row_loc_ID or loc.Row_loc_ID = bord.Row_loc_ID
            WHERE   ci.Currency = ''' + @Currency + '''';
			*/
            PRINT @DynamicQuery;
            EXEC (@DynamicQuery);
			
            DECLARE @Query NVARCHAR(1000) = 'SELECT @C = COUNT(*) FROM ' + @indvCurrencyTempTableName;
            DECLARE @Count AS INT;
            EXEC sp_executesql @Query, N'@C INT OUTPUT', @C = @Count OUTPUT;

            IF ( @Count > 0 )
                BEGIN
                    SET @csvCurrencySheetName = @csvCurrencySheetName + 'CARD_DETAIL_' + ISNULL(@Currency, 'null') + ',';
                    SET @csvCurrencyTablename = @csvCurrencyTablename + @indvCurrencyTempTableName + ',';
                    SET @csvDoNotIncludeColmuns = @csvDoNotIncludeColmuns + 'Rowid,CurrencyPageName|'; 
                END;
         
            FETCH NEXT FROM cur_Currency INTO @Currency;
        END;
		 
    CLOSE cur_Currency;
    DEALLOCATE cur_Currency;

 
    SET @csvWorkSheetName = @csvCurrencySheetName + 'All_FIS_Client_IDs,Client_Breakage,Payments';
    SET @csvTempTableName = @csvCurrencyTablename + '##tmpAllFisClientId,##tmpBreakageSummary,##tmpBreakagePayments';
    SET @csvDoNotIncludeColmuns = @csvDoNotIncludeColmuns + 'RowID|RowID,BreakageID,CurrencyPageName';   
-- Create Blank file 
    SET @DynamicQuery = 'EXEC [Dev_Swiftgift_DWH].[dbo].[Shared_Create_Blank_Excel_File] @FullFilename = ''' + @ExcelFilename + ''',
        @csvTempTableName = ''' + @csvTempTableName + ''',
        @csvWorkSheetName = ''' + @csvWorkSheetName + ''', 
        @csvDoNotIncludeColmuns = ''' + @csvDoNotIncludeColmuns + '''';
    EXEC (@DynamicQuery);
-- Copy Data       

    DECLARE @DataCopyQuery AS NVARCHAR(MAX);
	/* declare variables */
    DECLARE @cur_cu_TblName NVARCHAR(MAX)
    ,   @cur_cu_Sheetname NVARCHAR(MAX);
	
    DECLARE cr_dataCopyCurrency CURSOR FAST_FORWARD READ_ONLY
    FOR
    SELECT  a.Value, b.Value
    FROM    dbo.SG_Split(@csvCurrencyTablename, ',') a
    INNER JOIN dbo.SG_Split(@csvCurrencySheetName, ',') b ON b.valueid = a.valueid;	
    OPEN cr_dataCopyCurrency;
	
    FETCH NEXT FROM cr_dataCopyCurrency INTO @cur_cu_TblName, @cur_cu_Sheetname;
	
    WHILE @@FETCH_STATUS = 0
        BEGIN
            
            SET @DataCopyQuery = 'INSERT into OPENROWSET(
            ''Microsoft.ACE.OLEDB.12.0'', 
            ''Excel 8.0;Database=' + @ExcelFilename + ';HDR=yes'', 
            ''SELECT * FROM [' + @cur_cu_Sheetname
                + '$]'')
            SELECT  FISClientID,FISClientName,AcctEnding,[Proxy],[Date Created],[Currency],[Load],Breakage,[Expiration],
					[OMSI Order ID],[OMSI Program],[OMSI Location],[First Name],[Last Name],[Original Address 1],
            [Original Address 2], [Original City],[Original State],[Original ZipCode], [Original Country],[Client Key],[Client Unique ID],[Comment],PaymentRefNo ,
			[Registered FirstName], [Registered LastName], [Registered Addr1],
           [Registered Addr2], [Registered City],[Registered State],
           [Registered Zip],[Registered Country]
					FROM ' + @cur_cu_TblName + ' order by FISClientName,RowID';
            EXEC (@DataCopyQuery);
			
            EXEC(' IF OBJECT_ID(''tempdb..' + @cur_cu_TblName + ''') IS NOT NULL drop table ' + @cur_cu_TblName);
			

            FETCH NEXT FROM cr_dataCopyCurrency INTO @cur_cu_TblName, @cur_cu_Sheetname;
        END;

    CLOSE cr_dataCopyCurrency;
    DEALLOCATE cr_dataCopyCurrency;

    
    SET @DataCopyQuery = 'INSERT into OPENROWSET(
            ''Microsoft.ACE.OLEDB.12.0'', 
            ''Excel 8.0;Database=' + @ExcelFilename + ';HDR=yes'', 
            ''SELECT * FROM [All_FIS_Client_IDs$]'')
            SELECT  [FIS Client ID], [FIS Client Name], [FIS Subprogram ID], [FIS Subprogram Name], Currency,
                    [Total Cards], [Total Load], [Avg Load], [Net Breakage], [Breakage %], [Total Cards to Date], [Total Load to Date],
                    [Avg Load to Date], [Total Breakage to Date], [Breakage % to Date] FROM ##tmpAllFisClientId order by RowID';
    EXEC (@DataCopyQuery);
	
    
    SET @DataCopyQuery = 'INSERT into OPENROWSET(
            ''Microsoft.ACE.OLEDB.12.0'', 
            ''Excel 8.0;Database=' + @ExcelFilename
        + ';HDR=yes;'', 
            ''SELECT * FROM [Client_Breakage$]'')
            SELECT  [OMSI Client ID], [OMSI Client Name], [Breakage Report Name], [FIS Client ID], left([FIS Client Name],254) as [FIS Client Name], Currency,
                                            [Total Cards], [Total Load], [Avg Load], [Total Breakage], [Breakage %], [Vat Tax], [Shared Reissue Count],
                                            [Shared Reissue Total], [Net Swift Breakage],[Client Share Percentage], [Card Fee Deduction], [Unfunded Card Fee Deduction],
                                            [Client Reissue Count], [Client Reissue Total], [Additional Deductions], [Current Month Breakage], [Previous month Negative]
											, [Breakage Owed] FROM ##tmpBreakageSummary order by RowID';
    EXEC (@DataCopyQuery);
    
    SET @DataCopyQuery = 'INSERT into OPENROWSET(
            ''Microsoft.ACE.OLEDB.12.0'', 
            ''Excel 8.0;Database=' + @ExcelFilename + ';HDR=yes'', 
            ''SELECT * FROM [Payments$]'')
            SELECT  * FROM ##tmpBreakagePayments';
    EXEC (@DataCopyQuery);
    
-- Format File		
    DECLARE @FormatQuery VARCHAR(4000);
    SET @FormatQuery = 'S:\SqlExportedFiles\BreakageReport\FormatBreakageReport_AllClient.vbs "' + @ExcelFilename + '"';
    EXEC xp_cmdshell @FormatQuery;

	SET @SendEmailTo = ISNULL(@SendEmailTo,'OMSIReports@swiftprepaid.com;AUTOQA@DAVINCIPAY.COM')
	EXEC Dev_Swiftgift_DWH_EXT.dbo.Shared_send_dbmail @Id = N'REPORTS_BREAKAGE_ALL_CLIENT_SUMMARY',                      -- nvarchar(256)
	                                                  @EmailRecipients = @SendEmailTo,         -- nvarchar(max)
	                                                                 -- nvarchar(max)
	                                                  @EmailReplacementKeyVals =@EmailReplacementKeyVals, -- nvarchar(max)
	                                                  @file_attachments = @ExcelFilename
		
    DECLARE @FTPCommand VARCHAR(4000) = CASE WHEN @Environment = 'PRODUCTION'
                                             THEN 'S:\SFTPScripts\SFTP.Bat "' + @ExcelFilename + '" ' + ' "/FTP/OMSIFTP/ExportedFiles/Liventus/BreakageReports/' + DATENAME(MONTH,
                                                                                                                                                @CardExpirationLookup_StartDate)
                                                  + DATENAME(YEAR, @CardExpirationLookup_StartDate) + '"'
                                             ELSE ''
                                        END;
    --PRINT @FTPCommand;
    EXEC xp_cmdshell @FTPCommand;
       

  --  EXEC master.dbo.xp_fileexist @ExcelFilename, @isFileExists OUTPUT;

	
    --IF @isFileExists = 1
    --    BEGIN 
    --        SET @DeleteFileQuery = 'del "' + @ExcelFilename + '"';
    --        EXEC xp_cmdshell @DeleteFileQuery;
    --    END;  

    IF OBJECT_ID('tempdb..##tmpAllFisClientId') IS NOT NULL
        BEGIN 
            DROP TABLE  ##tmpAllFisClientId;
        END;

    IF OBJECT_ID('tempdb..##tmpBreakageSummary') IS NOT NULL
        BEGIN 
            DROP TABLE  ##tmpBreakageSummary;
        END;

    IF OBJECT_ID('tempdb..##tmpBreakagePayments') IS NOT NULL
        BEGIN 
            DROP TABLE  ##tmpBreakagePayments;
        END;
		
    IF OBJECT_ID('tempdb..#tmpBreakageDataDetails') IS NOT NULL
        BEGIN 
            DROP TABLE  #tmpBreakageDataDetails;
        END;

    IF OBJECT_ID('tempdb..#tmpNonMon') IS NOT NULL
        BEGIN 
            DROP TABLE  #tmpNonMon;
        END;
END; 

