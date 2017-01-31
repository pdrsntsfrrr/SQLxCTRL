SELECT
      'SchemaName'		= sc.name 
	, 'TableName'		= ob.name		
    , 'IndexName'		= ix.name
    , 'NumberofRows'	= si.rowcnt
    , 'ModifiedRows'	= si.rowmodctr /*modified rows after the last statistics updade */
    , 'Racio'			= CAST(100 * (si.rowmodctr * 1.0) / (si.rowcnt * 1.0) AS INT)
    , 'StatsAgeInDays'	= DATEDIFF (DAY, STATS_DATE(ob.object_id, st.stats_id), GETDATE())
    , 'StatsDate'		= STATS_DATE(ob.object_id, st.stats_id)
    , 'UpdateStatsCmd'	= '-- RAISERROR (''Updating ' + sc.name + '.' + ob.name + ' [' + ix.name + ']'', 10,1) WITH NOWAIT;'
        + 'UPDATE STATISTICS [' + sc.name + '].[' + ob.name + '] [' + ix.name + '] WITH FULLSCAN;'
        + 'RAISERROR (''Done!'', 10,1) WITH NOWAIT'
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
	AND CAST(100 * (si.rowmodctr * 1.0) / (si.rowcnt * 1.0) AS INT) > 0
ORDER BY
    CAST(100 * (si.rowmodctr * 1.0) / (si.rowcnt * 1.0) AS BIGINT) DESC;