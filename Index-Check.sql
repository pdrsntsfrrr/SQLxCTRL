/*
================================================================================================================

 [OBJECT NAME]: SQLxCtrl_IndexManualMaintenance


 [DESCRIPTION]: Index Manual Maintenance
 [CODE BY]: Pedro Ferreira

 [NOTES]:
    Note #1: http://www.sqlskills.com/blogs/paul/where-do-the-books-online-index-fragmentation-thresholds-come-from/
             "if an index has less than 1000 pages... don't bother removing fragmentation"


 [MODIFICATION HISTORY]:
     Date        Author                        Comment
     ──────────  ───────────────────────────   ────────────────────────────────────────────────────────────────
     01-01-2015  Pedro Ferreira                Inception
     10-09-2015  Pedro Ferreira                Optimizations

================================================================================================================
*/

USE xxx; -- Specify the database
GO

SET NOCOUNT ON;
GO

DECLARE @LowFragmentation  INT;
DECLARE @HighFragmentation INT;
DECLARE @PageCountLevel    INT;

SET @LowFragmentation  = 5;
SET @HighFragmentation = 100;
SET @PageCountLevel    = 1000; -- Note 1


SELECT
      'Database ID' = DB_ID()
    , 'Database Name' = DB_NAME()
    , 'Table Name' = t.name
    , 'Rows Qty' = p.rows
    , 'Index Name' = i.name
    , 'Fill Factor' =
        CASE
            WHEN i.fill_factor = 0 OR i.fill_factor = 100
                THEN 100
            ELSE i.fill_factor
        END
    , 'Page Fullness' = ips.avg_page_space_used_in_percent
    , 'Total Space MB' = a.total_pages * 8 * 1024 / 1000000
    , 'UsedSpaceKB' = a.used_pages * 8
    , 'UnusedSpaceKB' = (a.total_pages - a.used_pages) * 8
    , 'ReadsQty' = ius.user_seeks + ius.user_scans + ius.user_lookups/*number of times that the index help*/
    , 'WritesQty' = ius.user_updates/*number of times that the index dont help*/
    , 'Ratio' = (ius.user_seeks + ius.user_scans + ius.user_lookups) / COALESCE (NULLIF (ius.user_updates, 0), 1)/*Racio should be >=2*/
    , 'Frag' = ROUND (ips.avg_fragmentation_in_percent, 0)
    , 'Last Read' = COALESCE (ius.last_user_seek, ius.last_user_scan, ius.last_user_lookup)
    , 'Last Write' = ius.last_user_update
    , 'Drop CMD' = '-- DROP INDEX ' + i.name + ' ON ' + s.name + '.' + t.name + ' WITH (ONLINE=OFF)'
    , 'Disable CMD' = '-- ALTER INDEX ' + i.name + ' ON ' + s.name + '.' + t.name + ' DISABLE'
    , 'Kill Fragmentation CMD' =
        CASE
            WHEN ips.avg_fragmentation_in_percent > @HighFragmentation
                THEN '-- ALTER INDEX ' + i.name + ' ON ' + s.name + N'.' + t.name + ' REBUILD'
            WHEN ips.avg_fragmentation_in_percent > @LowFragmentation
                THEN '-- ALTER INDEX ' + i.name + ' ON ' + s.name + N'.' + t.name + ' REORGANIZE'
            ELSE ''
        END
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
        INNER JOIN sys.dm_db_index_physical_stats (DB_ID(), NULL, NULL, NULL, 'DETAILED') ips /* SAMPLED is quicker */
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
    , Ratio ASC;
GO

SET NOCOUNT OFF;
GO