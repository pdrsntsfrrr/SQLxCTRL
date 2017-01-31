SET NOCOUNT ON;
GO

DECLARE @SchemaName     AS nvarchar(128);
DECLARE @TableName      AS nvarchar(128);
DECLARE @IndexName      AS nvarchar(128);
DECLARE @NumberofRows   AS nvarchar(128);
DECLARE @ModifiedRows   AS nvarchar(128);
DECLARE @StatsDate      AS nvarchar(128);

DECLARE @Statement      AS nvarchar(400);
DECLARE @LogStatement   AS nvarchar(400);

DECLARE @PreviousGetDate    AS datetime2(7);
DECLARE @InitialTime        AS datetime2(7);

SET @InitialTime = GETDATE();

SET @LogStatement = CONVERT(nvarchar(24), GETDATE(), 120) + ' | COMMAND BLOCK START - UPDATE STATISTICS';
RAISERROR (@LogStatement, 10, 1) WITH NOWAIT;
RAISERROR ('---------------------------------------------------------------------------------------------', 10, 1) WITH NOWAIT;

DECLARE curUpdateStats CURSOR FOR
    SELECT
          'SchemaName'	 = sc.name
	    , 'TableName'	 = ob.name
        , 'IndexName'	 = ix.name
        , 'NumberofRows' = si.rowcnt
        , 'ModifiedRows' = si.rowmodctr /*modified rows after the last statistics updade */
        , 'StatsDate'	 = CONVERT(nvarchar(128), STATS_DATE(ob.object_id, st.stats_id), 120)
    FROM
        sys.objects AS ob WITH (NOLOCK)
        JOIN sys.indexes AS ix WITH (NOLOCK)
          ON ix.object_id = ob.object_id
        JOIN sys.stats AS st WITH (NOLOCK)
          ON st.object_id = ob.object_id AND st.stats_id = ix.index_id
        JOIN sys.schemas AS sc WITH (NOLOCK)
          ON sc.schema_id = ob.schema_id
        JOIN sys.sysindexes AS si WITH (NOLOCK)
          ON si.id = ob.object_id AND si.indid = ix.index_id
    WHERE 1 = 1
        AND ob.type = 'U' /* U = Table (user-defined) */
        AND si.rowcnt > 0
        AND si.rowmodctr > 0 /* to check all the stats with modified rows */
	    --AND CAST(100 * (si.rowmodctr * 1.0) / (si.rowcnt * 1.0) AS INT) > 0 /* Racio between modified and total of rows */
    ORDER BY
        CAST(100 * (si.rowmodctr * 1.0) / (si.rowcnt * 1.0) AS BIGINT) DESC;
OPEN curUpdateStats;

FETCH NEXT FROM curUpdateStats INTO @SchemaName, @TableName, @IndexName, @NumberofRows, @ModifiedRows, @StatsDate

WHILE (@@FETCH_STATUS = 0)
BEGIN

    SET @PreviousGetDate = GETDATE();

    SET @Statement = 'UPDATE STATISTICS [' + @SchemaName + '].[' + @TableName + '] [' + @IndexName + '] WITH FULLSCAN;'; /*WITH RESAMPLE*/

    SET @LogStatement = CONVERT(nvarchar(24), @PreviousGetDate, 120) + ' | COMMAND  - ' + @Statement;
    RAISERROR (@LogStatement, 10, 1) WITH NOWAIT;

    SET @LogStatement = '                    | REASON   - ' + @ModifiedRows + ' modified rows of ' + @NumberofRows + ' (last update: ' + @StatsDate + ')';
    RAISERROR (@LogStatement, 10, 1) WITH NOWAIT;

	BEGIN TRY
		EXEC sp_executesql @Statement;
	END TRY
	BEGIN CATCH
		PRINT error_message()
	END CATCH

    SET @LogStatement = '                    | DURATION - ' + CAST(DATEDIFF(SECOND, @PreviousGetDate, GETDATE()) AS varchar(20)) + ' seconds';
    RAISERROR (@LogStatement, 10, 1) WITH NOWAIT;

    FETCH NEXT FROM curUpdateStats INTO @SchemaName, @TableName, @IndexName, @NumberofRows, @ModifiedRows, @StatsDate;
END

CLOSE curUpdateStats;
DEALLOCATE curUpdateStats;


RAISERROR ('---------------------------------------------------------------------------------------------', 10, 1) WITH NOWAIT;
SET @LogStatement = CONVERT(nvarchar(24), GETDATE(), 120) + ' | COMMAND BLOCK FINISH - UPDATE STATISTICS (block done in ' + CAST(DATEDIFF(SECOND, @InitialTime, GETDATE()) AS varchar(20)) + ' seconds)';
RAISERROR (@LogStatement, 10, 1) WITH NOWAIT;

SET NOCOUNT OFF;
GO