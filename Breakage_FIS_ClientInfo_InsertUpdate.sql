USE [Dev_Swiftgift_DWH]
GO

/****** Object:  StoredProcedure [dbo].[Breakage_FIS_ClientInfo_InsertUpdate]    Script Date: 10/21/2020 11:59:00 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [dbo].[Breakage_FIS_ClientInfo_InsertUpdate] @RunDate DATE
AS
    BEGIN
	
        DECLARE @HeaderId BIGINT; 

        SELECT  @HeaderId = HeaderID
        FROM    dbo.FIS_Import_NonMonetary_Header WITH ( NOLOCK ) WHERE WorkOfDate = @RunDate;
		
        IF OBJECT_ID('tempdb..#tmpTblFisClientName') IS NOT NULL
            BEGIN 
                DROP TABLE #tmpTblFisClientName;
            END; 
        SELECT DISTINCT
                IssuerClientID ,
                ClientName ,
                SubProgramID ,
                BINCurrencyAlpha
        INTO    #tmpTblFisClientName
        FROM    dbo.FIS_Import_NonMonetary_Detail WITH ( NOLOCK )
        WHERE   HeaderID = @HeaderId; 

        INSERT  INTO dbo.Breakage_FISClient_Info
                ( FISClientID ,
                  SubprogramID ,
                  Currency ,
                  FISClientName
                )
                SELECT DISTINCT
                        IssuerClientID ,
                        SubProgramID ,
                        BINCurrencyAlpha ,
                        ClientName
                FROM    #tmpTblFisClientName
                WHERE   CONVERT(NVARCHAR(10),IssuerClientID) + '-' + CONVERT(NVARCHAR(10), SubProgramID) NOT IN (
                        SELECT DISTINCT
                                FISClientID + '-' + SubprogramID
                        FROM    dbo.Breakage_FISClient_Info WITH ( NOLOCK ) );

        UPDATE  Breakage_FISClient_Info
        SET     FISClientName = b.ClientName
        FROM    dbo.Breakage_FISClient_Info a WITH ( NOLOCK )
                INNER JOIN #tmpTblFisClientName b ON a.FISClientID = b.IssuerClientID;

              
        IF OBJECT_ID('tempdb..#tmpTblFisClientName') IS NOT NULL
            BEGIN 
                DROP TABLE #tmpTblFisClientName;
            END;
        
			


    END;	
GO


