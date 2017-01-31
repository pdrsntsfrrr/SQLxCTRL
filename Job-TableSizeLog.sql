SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


/* --- Create table for log the data --- */

USE [AdminDB]
GO

IF NOT OBJECT_ID('[dbo].[TableSizeLog]') IS NULL
BEGIN
    DROP TABLE [dbo].[TableSizeLog];
END

CREATE TABLE [dbo].[TableSizeLog](
    [IDTableSizeLog]    [int] IDENTITY(1,1) NOT NULL,
	[ServerName]        [varchar](100) NULL,
	[DatabaseName]      [varchar](100) NULL,
	[SchemaName]        [varchar](100) NOT NULL,
	[TableName]         [varchar](100) NOT NULL,
	[RowsCount]         [bigint] NOT NULL,
	[ColumnsCount]      [bigint] NOT NULL,
	[ReservedSizeKB]    [bigint] NOT NULL,
	[DataSizeKB]        [bigint] NOT NULL,
	[IndexSizeKB]       [bigint] NOT NULL,
	[UnusedSizeKB]      [bigint] NOT NULL,
	[LogDate]           [smalldatetime] NOT NULL,
 CONSTRAINT [PK_TableSizeLog] PRIMARY KEY CLUSTERED
(
	[IDTableSizeLog] ASC
) WITH
    ( PAD_INDEX = OFF
    , STATISTICS_NORECOMPUTE = OFF
    , IGNORE_DUP_KEY = OFF
    , ALLOW_ROW_LOCKS = ON
    , ALLOW_PAGE_LOCKS = ON
    ) ON [PRIMARY]
) ON [PRIMARY]
GO


/* --- Create SQL JOB --- */

USE [msdb]
GO


IF EXISTS (SELECT job_id FROM msdb.dbo.sysjobs WHERE name = N'MAINTENANCE: Log table size')
EXEC msdb.dbo.sp_delete_job @job_name=N'MAINTENANCE: Log table size', @delete_unused_schedule=1


DECLARE @CurrentSQLUser AS VARCHAR(MAX)
SET @CurrentSQLUser = suser_sname()

DECLARE @jobId BINARY(16)
EXEC msdb.dbo.sp_add_job
        @job_name = N'MAINTENANCE: Log table size',
        @enabled = 1,
		@notify_level_eventlog = 0,
		@notify_level_email = 2,
		@notify_level_netsend = 2,
		@notify_level_page = 2,
		@delete_level = 0,
		@description = N'Log the table size of every user databases on the instance',
		@category_name = N'Database Maintenance',
		@owner_login_name = @CurrentSQLUser,
        @job_id = @jobId OUTPUT
GO

DECLARE @CurrentSQLServer AS VARCHAR(MAX)
SET @CurrentSQLServer = @@SERVERNAME

EXEC msdb.dbo.sp_add_jobserver @job_name = N'MAINTENANCE: Log table size',
        @server_name = @CurrentSQLServer
GO


EXEC msdb.dbo.sp_add_jobstep
        @job_name = N'MAINTENANCE: Log table size',
        @step_name = N'Primary replica?',
		@step_id = 1,
		@cmdexec_success_code = 0,
		@on_success_action = 3,
		@on_fail_action = 1,
		@retry_attempts = 0,
		@retry_interval = 0,
		@os_run_priority = 0,
        @subsystem = N'TSQL',
		@command = N'IF EXISTS(SELECT 1 FROM master.sys.dm_hadr_availability_replica_states WHERE is_local = 1 AND role_desc  = ''PRIMARY'')
  OR (SELECT COUNT(replica_id) FROM master.sys.dm_hadr_availability_replica_states) = 0
    BEGIN
        PRINT ''Server is a PRIMARY Node'';
    END
ELSE
    BEGIN
        THROW 51000, ''Ignoring Execution as Server is not PRIMARY.'', 1;
    END;',
		@database_name = N'master',
		@flags = 0
GO

EXEC msdb.dbo.sp_add_jobstep
        @job_name = N'MAINTENANCE: Log table size',
        @step_name = N'Log table sizes',
		@step_id = 2,
		@cmdexec_success_code = 0,
		@on_success_action = 1,
		@on_fail_action = 2,
		@retry_attempts = 0,
		@retry_interval = 0,
		@os_run_priority = 0,
        @subsystem = N'TSQL',
		@command = N'SET NOCOUNT ON

DECLARE @RowIndex INT
      , @RowCount INT
      , @Name VARCHAR(300)
      , @SQLQuery VARCHAR(MAX)
      , @DBLog VARCHAR(300)

IF NOT OBJECT_ID(''tempdb..#tempCheckDatabase_pfe08'') IS NULL
BEGIN
    DROP TABLE #tempCheckDatabase_pfe08;
END

SELECT
      [RowID]  = Identity(INT ,1 ,1)
    , [DBName] = NAME
INTO #tempCheckDatabase_pfe08
FROM sys.databases WITH (NOLOCK)
WHERE 1 = 1
    AND NAME NOT IN (''master'', ''tempdb'', ''model'', ''msdb'', ''ReportServer'', ''ReportServerTempDB'')

SET @RowCount = @@RowCount;
SET @RowIndex = 1;
SET @DBLog = ''[AdminDB]'';

WHILE @RowIndex < =  @RowCount
BEGIN
    SELECT @Name = DBName
    FROM #tempCheckDatabase_pfe08 WITH (NOLOCK)
    WHERE RowID = @RowIndex;

    SELECT @SQLQuery = ''USE '' + @Name +'';
    INSERT INTO '' + @DBLog + ''.[dbo].[TableSizeLog]
    SELECT
      [ServerName]     = @@SERVERNAME
    , [DatabaseName]   = DB_NAME()
    , [SchemaName]     = sch.name
    , [TableName]      = obj.name
    , [RowsCount]      = pstats.rows
    , [ColumnsCount]   = (SELECT COUNT(1) FROM sys.columns AS C WHERE C.object_id = obj.object_id)
    , [ReservedSizeKB] = (pstats.reserved + ISNULL(inttbl.reserved, 0)) * 8
    , [DataSizeKB]     = pstats.data * 8
    , [IndexSizeKB]    = (CASE WHEN (pstats.used + ISNULL(inttbl.used, 0)) > pstats.data THEN (pstats.used + ISNULL(inttbl.used, 0)) - pstats.data ELSE 0 END) * 8
    , [UnusedSizeKB]   = (CASE WHEN (pstats.reserved + ISNULL(inttbl.reserved, 0)) > pstats.used THEN (pstats.reserved + ISNULL(inttbl.reserved, 0)) - pstats.used ELSE 0 END) * 8
    , [LogDate]        = GETDATE()
    FROM
            (
            SELECT
                ps.object_id
                , [rows]     = SUM(CASE WHEN (ps.index_id < 2) THEN row_count ELSE 0 END)
                , [reserved] = SUM(ps.reserved_page_count)
                , [data]     = SUM(
                                CASE
                                WHEN (ps.index_id < 2) THEN (ps.in_row_data_page_count + ps.lob_used_page_count + ps.row_overflow_used_page_count)
                                ELSE (ps.lob_used_page_count + ps.row_overflow_used_page_count)
                                END)
                , [used]     = SUM(ps.used_page_count)
            FROM sys.dm_db_partition_stats ps
            GROUP BY ps.object_id
            ) AS pstats
        LEFT OUTER JOIN
            (
            SELECT
                it.parent_id
                , [reserved] = SUM(ps.reserved_page_count)
                , [used]     = SUM(ps.used_page_count)
            FROM sys.dm_db_partition_stats ps
                INNER JOIN sys.internal_tables it
                    ON it.object_id = ps.object_id
            WHERE it.internal_type IN (202, 204)
            GROUP BY it.parent_id
            ) AS inttbl
            ON inttbl.parent_id = pstats.object_id
        INNER JOIN sys.all_objects obj
            ON pstats.object_id = obj.object_id
        INNER JOIN sys.schemas sch
            ON obj.schema_id = sch.schema_id
    WHERE 1 = 1
        AND obj.type <> N''''S''''
        AND obj.type <> N''''IT''''
    ORDER BY
        pstats.rows DESC
    --  , sch.name ASC
    --  , obj.name ASC;

    '';
    PRINT @SQLQuery;
    EXEC (@SQLQuery);

    SET @RowIndex = @RowIndex + 1;
    SET @SQLQuery = '''';
END

SET NOCOUNT OFF',
		@database_name = N'master',
		@flags = 0
GO

DECLARE @CurrentSQLUser AS VARCHAR(MAX)
SET @CurrentSQLUser = suser_sname()

EXEC msdb.dbo.sp_update_job
        @job_name = N'MAINTENANCE: Log table size',
		@enabled = 1,
		@start_step_id = 1,
		@notify_level_eventlog = 0,
		@notify_level_email = 2,
		@notify_level_netsend = 2,
		@notify_level_page = 2,
		@delete_level = 0,
		@description = N'Log the table size of every user databases on the instance',
		@category_name = N'Database Maintenance',
		@owner_login_name = @CurrentSQLUser,
		@notify_email_operator_name = N'',
		@notify_netsend_operator_name = N'',
		@notify_page_operator_name = N''
GO


/* --- Add schedule for the SQL Job --- */

DECLARE @schedule_id INT

EXEC msdb.dbo.sp_add_jobschedule
        @job_name = N'MAINTENANCE: Log table size',
        @name = N'Every day at 20:00',
		@enabled = 1,
		@freq_type = 4,
		@freq_interval = 1,
		@freq_subday_type = 1,
		@freq_subday_interval = 0,
		@freq_relative_interval = 0,
		@freq_recurrence_factor = 1,
		@active_start_date = 20170126,
		@active_end_date = 99991231,
		@active_start_time = 200000,
		@active_end_time = 235959,
        @schedule_id = @schedule_id OUTPUT
GO