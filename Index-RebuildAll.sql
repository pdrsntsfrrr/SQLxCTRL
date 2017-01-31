SET NOCOUNT ON;
GO

DECLARE @SchemaName AS nvarchar(128);
DECLARE @TableName  AS nvarchar(128);
DECLARE @IndexName  AS nvarchar(128);

DECLARE @NumberofRows   AS nvarchar(128);
DECLARE @FillFactor     AS nvarchar(128);
DECLARE @TotalSpaceMB   AS nvarchar(128);
DECLARE @Ratio          AS nvarchar(128);
DECLARE @Frag           AS nvarchar(128);

DECLARE @Statement AS nvarchar(400);
DECLARE @LogStatement AS nvarchar(400);
DECLARE @PreviousGetDate AS datetime2(7);
DECLARE @InitialTime AS datetime2(7);

DECLARE @LowFragmentation  AS INT;
DECLARE @HighFragmentation AS INT;
DECLARE @PageCountLevel    AS INT;

SET @LowFragmentation  = 5;
SET @HighFragmentation = 100;
SET @PageCountLevel    = 1000;

SET @InitialTime = GETDATE();

SET @LogStatement = CONVERT(nvarchar(24), GETDATE(), 120) + ' | COMMAND BLOCK START - INDEX MAINTENANCE';
RAISERROR (@LogStatement, 10, 1) WITH NOWAIT;
RAISERROR ('---------------------------------------------------------------------------------------------', 10, 1) WITH NOWAIT;

DECLARE curIndexMaintenance CURSOR FOR
SELECT
	  'SchemaName' = s.name
	, 'TableName' = t.name
	, 'IndexName' = i.name
    , 'NumberofRows' = p.rows
    , 'FillFactor' =
        CASE
            WHEN i.fill_factor = 0 OR i.fill_factor = 100
                THEN 100
            ELSE i.fill_factor
        END
    , 'TotalSpaceMB' = a.total_pages * 8 * 1024 / 1000000
    , 'Ratio' = (ius.user_seeks + ius.user_scans + ius.user_lookups) / COALESCE (NULLIF (ius.user_updates, 0), 1)/*Racio should be >=2*/
    , 'Frag' = ROUND (ips.avg_fragmentation_in_percent, 0)
FROM
    sys.dm_db_index_usage_stats ius WITH (NOLOCK)
        INNER JOIN sys.indexes i WITH (NOLOCK)
            ON i.index_id = ius.index_id
                AND ius.object_id = i.object_id
        INNER JOIN sys.partitions p WITH (NOLOCK)
            ON p.index_id = ius.index_id
                AND ius.object_id = p.object_id
        INNER JOIN sys.tables t WITH (NOLOCK)
            ON t.object_id = i.object_id
        INNER JOIN sys.schemas s WITH (NOLOCK)
            ON s.schema_id = t.schema_id
        INNER JOIN sys.allocation_units a WITH (NOLOCK)
            ON p.partition_id = a.container_id
        INNER JOIN sys.dm_db_index_physical_stats (DB_ID(), NULL, NULL, NULL, 'SAMPLED') ips /* SAMPLED is quicker */
            ON ips.index_id = i.index_id
                AND ips.object_id = i.object_id
WHERE 1 = 1
    AND ius.database_id = DB_ID()
    AND i.type > 0  -- Index Type: 0=HEAP ; 1=CLUSTERED ; 2=NONCLUSTERED
    AND i.is_disabled = 0 /*ignore disabled indexes*/
    AND i.is_hypothetical = 0 /*ignore hypothetical indexes*/
    AND a.type = 1  -- Allocation Unit Type: 1=IN_ROW_DATA ; 2=LOB_DATA ; 3=ROW_OVERFLOW_DATA
    AND ips.page_count > @PageCountLevel
	AND ips.avg_fragmentation_in_percent > @LowFragmentation
    --AND ips.index_level = 0 /* to check only the leaf level */
    --AND p.rows > 5000
    --AND i.is_primary_key = 1
    --AND i.is_unique_constraint = 0
ORDER BY
      p.rows DESC
    , Ratio  ASC;

OPEN curIndexMaintenance;

FETCH NEXT FROM curIndexMaintenance INTO @SchemaName, @TableName, @IndexName, @NumberofRows, @FillFactor, @TotalSpaceMB, @Ratio, @Frag

WHILE (@@FETCH_STATUS = 0)
BEGIN
    SET @PreviousGetDate = GETDATE();

    SET @Statement = 'ALTER INDEX [' + @IndexName + '] ON [' + @SchemaName + '].[' + @TableName + '] REORGANIZE;';

    SET @LogStatement = CONVERT(nvarchar(24), @PreviousGetDate, 120) + ' | COMMAND  » ' + @Statement;
    RAISERROR (@LogStatement, 10, 1) WITH NOWAIT;

    SET @LogStatement = '                    | REASON   » Frag: ' + @Frag + '    RowQty (size in MB): ' + @NumberofRows + '(' + @TotalSpaceMB + ')    Racio R/W: ' + @Ratio + ' with a FF: ' + @FillFactor;
    RAISERROR (@LogStatement, 10, 1) WITH NOWAIT;

    BEGIN TRY
		EXEC sp_executesql @Statement;
	END TRY
	BEGIN CATCH
		PRINT error_message()
	END CATCH

    SET @LogStatement = '                    | DURATION » ' + CAST(DATEDIFF(SECOND, @PreviousGetDate, GETDATE()) AS varchar(20)) + ' seconds';
    RAISERROR (@LogStatement, 10, 1) WITH NOWAIT;
    FETCH NEXT FROM curIndexMaintenance INTO @SchemaName, @TableName, @IndexName, @NumberofRows, @FillFactor, @TotalSpaceMB, @Ratio, @Frag;
END

CLOSE curIndexMaintenance;
DEALLOCATE curIndexMaintenance;


RAISERROR ('---------------------------------------------------------------------------------------------', 10, 1) WITH NOWAIT;
SET @LogStatement = CONVERT(nvarchar(24), GETDATE(), 120) + ' | COMMAND BLOCK FINISH - INDEX MAINTENANCE (block done in ' + CAST(DATEDIFF(SECOND, @InitialTime, GETDATE()) AS varchar(20)) + ' seconds)';
RAISERROR (@LogStatement, 10, 1) WITH NOWAIT;

SET NOCOUNT OFF;
GO
