--alter line number 24 to 41 for variables


set nocount on
----=====Declare Variables=====----
declare @servername nvarchar(50) = @@servername --used while creating jobs
declare @databases table (name nvarchar(200)) --temp table variable to store manually entered dbs
declare @exceptiondatabases table (name nvarchar(200))
declare @database_names varchar(200) --parsing manually entered dbs
declare @name varchar(50) -- database name for cursors
declare @auditfilepath nvarchar(200) --audit file path
declare @servercmd varchar(8000) -- command to create audit
declare @databasecmd varchar(8000) --Command to create database audit
declare @audittablecmd varchar(8000) -- Command to create audit table in management DB
declare @mgmtdb varchar(200) -- Management DB
declare @jobCMD_step1 nvarchar(max) --Job creation syntax was too big so this is part 1
declare @jobCMD_step2 nvarchar(max) -- This is part 2
declare @jobOwner nvarchar(50) -- Job owner, usually sa so that is set as default
declare @deploy bit --1 to deploy, 0 to rollback
declare @pDelimiter CHAR(1) = ',' -- used in parsing manually entered dbs, delimiting it by comma
declare @debug bit -- to debug manually entered dbs incase something is not working as intended
declare @debugOverrideAll bit -- to debug All dbs incase something is not working as intended

----=====Set Variables=====----
set @deploy = 0
set @mgmtdb = 'abc'
set @auditfilepath = N'C:\temp\log'
--set @database_names = 'StackOverflow, stackoverflow_dw, syra, abc, asbd, tempdb, Distribution'
set @database_names = 'ALL'
set @jobOwner = '[sa]'
set @debug = 0
set @debugOverrideAll = 0



/* Databases to be ignored start*/
insert into @exceptiondatabases values ('tempdb'), ('master'), ('msdb'), ('model'), ('Distribution')
/* Databases to be ignored end*/


   ----=====The End=====----

if @database_names = 'ALL' and @debugOverrideAll = 0 
begin
	set @debug = 0
	insert into @databases select name from sys.sysdatabases 


end
else if @database_names = 'ALL' and @debugOverrideAll = 1
	set @debug = 1
else if @database_names <> 'ALL' and @debugOverrideAll = 1 or @debug = 1

	begin

		--	/* taken from delimitedsplit8k function */

		WITH E1(N) AS (
						 SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL
						 SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL
						 SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1
						),                          --10E+1 or 10 rows
			   E2(N) AS (SELECT 1 FROM E1 a, E1 b), --10E+2 or 100 rows
			   E4(N) AS (SELECT 1 FROM E2 a, E2 b), --10E+4 or 10,000 rows max
		 cteTally(N) AS (--==== This provides the "base" CTE and limits the number of rows right up front
							 -- for both a performance gain and prevention of accidental "overruns"
						 SELECT TOP (ISNULL(DATALENGTH(@database_names),0)) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) FROM E4
						),
		cteStart(N1) AS (--==== This returns N+1 (starting position of each "element" just once for each delimiter)
						 SELECT 1 UNION ALL
						 SELECT t.N+1 FROM cteTally t WHERE SUBSTRING(@database_names,t.N,1) = @pDelimiter
						),
		cteLen(N1,L1) AS(--==== Return start and length (for use in substring)
						 SELECT s.N1,
								ISNULL(NULLIF(CHARINDEX(@pDelimiter,@database_names,s.N1),0)-s.N1,8000)
						   FROM cteStart s
						)
		--===== Do the actual split. The ISNULL/NULLIF combo handles the length for the final element when no delimiter is found.

		insert into @databases SELECT 
				Item       = trim(SUBSTRING(@database_names, l.N1, l.L1))
		   FROM cteLen l
		   /* end of delimitedsplit8k code */
		   set @debug = 1

		if @debug = 1 
		begin 
			select name [database_names_fed] from  @databases 
			select name [databases_that_dont_exist] from @databases where name not in (select name  from sys.sysdatabases)
			select name [databases_that_are_in_exception_list_therefore_ignored] from @databases where name  in (select name from @exceptiondatabases)
		end
		
	end

if @deploy = 1
begin
	set @serverCMD = '
			USE [master]
			GO

			begin try

			CREATE SERVER AUDIT [Audit]
			TO FILE 
			(	FILEPATH = '''+@auditfilepath +'''
				,MAXSIZE = 1024 MB
				,MAX_ROLLOVER_FILES = 2147483647
				,RESERVE_DISK_SPACE = OFF
			) WITH (QUEUE_DELAY = 1000, ON_FAILURE = CONTINUE)
			WHERE schema_name = ''dbo''

			end try
			begin catch 
			EXEC sp_configure ''show advanced options'', ''1''
			RECONFIGURE
			-- this enables xp_cmdshell
			EXEC sp_configure ''xp_cmdshell'', ''1'' 
			RECONFIGURE
			exec xp_cmdshell ''mkdir '+ @auditfilepath+ '''
			EXEC sp_configure ''show advanced options'', ''1''
			RECONFIGURE
			-- this enables xp_cmdshell
			EXEC sp_configure ''xp_cmdshell'', ''0''
			RECONFIGURE

			CREATE SERVER AUDIT [Audit]
			TO FILE 
			(	FILEPATH = '''+@auditfilepath+'''
				,MAXSIZE = 1024 MB
				,MAX_ROLLOVER_FILES = 2147483647
				,RESERVE_DISK_SPACE = OFF
			) WITH (QUEUE_DELAY = 1000, ON_FAILURE = CONTINUE)
			WHERE schema_name = ''dbo''

			end catch

			GO


			/* Enable server level audit*/

			USE [master]
			GO

			ALTER SERVER AUDIT [audit]
			with (STATE = ON)
			'
print @serverCMD

	DECLARE db_cursor CURSOR FOR 
	SELECT d.name 
	FROM @databases d join sys.sysdatabases sd on d.name = sd.name and d.name not in (select name from @exceptiondatabases)

	OPEN db_cursor  
	FETCH NEXT FROM db_cursor INTO @name  

	WHILE @@FETCH_STATUS = 0  
	BEGIN  
	set @databaseCMD = '

			USE ['+@name+']

			GO

			CREATE DATABASE AUDIT SPECIFICATION [DatabaseAuditSpecification]
			FOR SERVER AUDIT [audit]
			ADD (INSERT ON DATABASE::['+@name+'] BY [dbo]),
			ADD (DELETE ON DATABASE::['+@name+'] BY [dbo]),
			ADD (SELECT ON DATABASE::['+@name+'] BY [dbo]),
			ADD (UPDATE ON DATABASE::['+@name+'] BY [dbo]),
			ADD (SCHEMA_OBJECT_CHANGE_GROUP)

			GO 
		
			USE ['+@name+']

			GO

			ALTER DATABASE AUDIT SPECIFICATION [DatabaseAuditSpecification]
			with (STATE = ON)

			GO
			'
			Print @databaseCMD
		  FETCH NEXT FROM db_cursor INTO @name 
	END 

	CLOSE db_cursor  
	DEALLOCATE db_cursor 


	set @audittableCMD = '
	USE ['+@mgmtdb+']
	GO

	
	SET ANSI_NULLS ON
	GO

	SET QUOTED_IDENTIFIER ON
	GO

	CREATE TABLE [dbo].[audittable](
		[event_time] [datetime2](7) NOT NULL,
		[sequence_number] [int] NOT NULL,
		[action_id] [varchar](4) NULL,
		[succeeded] [bit] NOT NULL,
		[permission_bitmask] [varbinary](16) NOT NULL,
		[is_column_permission] [bit] NOT NULL,
		[session_id] [smallint] NOT NULL,
		[server_principal_id] [int] NOT NULL,
		[database_principal_id] [int] NOT NULL,
		[target_server_principal_id] [int] NOT NULL,
		[target_database_principal_id] [int] NOT NULL,
		[object_id] [int] NOT NULL,
		[class_type] [varchar](2) NULL,
		[session_server_principal_name] [nvarchar](128) NULL,
		[server_principal_name] [nvarchar](128) NULL,
		[server_principal_sid] [varbinary](85) NULL,
		[database_principal_name] [nvarchar](128) NULL,
		[target_server_principal_name] [nvarchar](128) NULL,
		[target_server_principal_sid] [varbinary](85) NULL,
		[target_database_principal_name] [nvarchar](128) NULL,
		[server_instance_name] [nvarchar](128) NULL,
		[database_name] [nvarchar](128) NULL,
		[schema_name] [nvarchar](128) NULL,
		[object_name] [nvarchar](128) NULL,
		[statement] [nvarchar](4000) NULL,
		[additional_information] [nvarchar](4000) NULL,
		[file_name] [nvarchar](260) NOT NULL,
		[audit_file_offset] [bigint] NOT NULL,
		[user_defined_event_id] [smallint] NOT NULL,
		[user_defined_information] [nvarchar](4000) NULL,
		[audit_schema_version] [int] NOT NULL,
		[sequence_group_id] [varbinary](85) NULL,
		[transaction_id] [bigint] NOT NULL,
		[client_ip] [nvarchar](128) NULL,
		[application_name] [nvarchar](128) NULL,
		[duration_milliseconds] [bigint] NOT NULL,
		[response_rows] [bigint] NOT NULL,
		[affected_rows] [bigint] NOT NULL,
		[connection_id] [uniqueidentifier] NULL,
		[data_sensitivity_information] [nvarchar](4000) NULL,
		[host_name] [nvarchar](128) NULL
	) ON [PRIMARY]
	GO

	USE ['+@mgmtdb+']

	GO

	CREATE CLUSTERED INDEX [ClusteredIndex_eventtime] ON [dbo].[audittable]
	(
		[event_time] ASC
	)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)

	GO
	'
	print @audittableCMD


	set @jobCMD_step1 = '
					USE [msdb]
					GO
					DECLARE @jobId BINARY(16)
					EXEC  msdb.dbo.sp_add_job @job_name=N''SecurityAudit_Merge_Cleanup'', 
							@enabled=1, 
							@notify_level_eventlog=0, 
							@notify_level_email=2, 
							@notify_level_page=2, 
							@delete_level=0, 
							@category_name=N''Database Maintenance'', 
							@owner_login_name='+@JobOwner+', @job_id = @jobId OUTPUT
					select @jobId
					GO
					EXEC msdb.dbo.sp_add_jobserver @job_name=N''SecurityAudit_Merge_Cleanup'', @server_name = '+ @servername+'
					GO
					USE [msdb]
					GO
					EXEC msdb.dbo.sp_add_jobstep @job_name=N''SecurityAudit_Merge_Cleanup'', @step_name=N''Merge_Step'', 
							@step_id=1, 
							@cmdexec_success_code=0, 
							@on_success_action=3, 
							@on_fail_action=2, 
							@retry_attempts=0, 
							@retry_interval=0, 
							@os_run_priority=0, @subsystem=N''TSQL'', 
							@command=N''
					use ['+@mgmtdb+']
					go

					;with cte as (SELECT *  FROM sys.fn_get_audit_file ('''''+@auditfilepath+'\*.sqlaudit'''',default,default) where schema_name = ''''dbo'''')

					MERGE audittable AS Target
					USING cte	AS Source
					ON Source.event_time = Target.event_time
					WHEN NOT MATCHED BY Target THEN
						INSERT (
	
						event_time
					, sequence_number
					, action_id
					, succeeded
					, permission_bitmask
					, is_column_permission
					, session_id
					, server_principal_id
					, database_principal_id
					, target_server_principal_id
					, target_database_principal_id
					, object_id
					, class_type
					, session_server_principal_name
					, server_principal_name
					, server_principal_sid
					, database_principal_name
					, target_server_principal_name
					, target_server_principal_sid
					, target_database_principal_name
					, server_instance_name
					, database_name
					, schema_name
					, object_name
					, statement
					, additional_information
					, file_name
					, audit_file_offset
					, user_defined_event_id
					, user_defined_information
					, audit_schema_version
					, sequence_group_id
					, transaction_id
					, client_ip
					, application_name
					, duration_milliseconds
					, response_rows
					, affected_rows
					, connection_id
					, data_sensitivity_information
					, host_name
	
						) 
						VALUES (source.event_time
					, source.sequence_number
					, source.action_id
					, source.succeeded
					, source.permission_bitmask
					, source.is_column_permission
					, source.session_id
					, source.server_principal_id
					, source.database_principal_id
					, source.target_server_principal_id
					, source.target_database_principal_id
					, source.object_id
					, source.class_type
					, source.session_server_principal_name
					, source.server_principal_name
					, source.server_principal_sid
					, source.database_principal_name
					, source.target_server_principal_name
					, source.target_server_principal_sid
					, source.target_database_principal_name
					, source.server_instance_name
					, source.database_name
					, source.schema_name
					, source.object_name
					, source.statement
					, source.additional_information
					, source.file_name
					, source.audit_file_offset
					, source.user_defined_event_id
					, source.user_defined_information
					, source.audit_schema_version
					, source.sequence_group_id
					, source.transaction_id
					, source.client_ip
					, source.application_name
					, source.duration_milliseconds
					, source.response_rows
					, source.affected_rows
					, source.connection_id
					, source.data_sensitivity_information
					, source.host_name);

					'', 
							@database_name=N''master'', 
							@flags=0
					GO

					'

				set @jobCMD_step2 = '
					USE [msdb]
					GO
					EXEC msdb.dbo.sp_add_jobstep @job_name=N''SecurityAudit_Merge_Cleanup'', @step_name=N''File_Cleanup'', 
							@step_id=2, 
							@cmdexec_success_code=0, 
							@on_success_action=1, 
							@on_fail_action=2, 
							@retry_attempts=0, 
							@retry_interval=0, 
							@os_run_priority=0, @subsystem=N''TSQL'', 
							@command=N''EXEC sp_configure ''''show advanced options'''', ''''1''''
					RECONFIGURE
					-- this enables xp_cmdshell
					EXEC sp_configure ''''xp_cmdshell'''', ''''1'''' 
					RECONFIGURE

					DECLARE @result int;  
					EXEC @result = xp_cmdshell ''''cd '+@auditfilepath+' & for /f "skip=5 eol=: delims=" %F in (''''''''dir /b /o-d /a-d *.sqlaudit'''''''') do @del "%F"'''';  
					IF (@result = 0)  
					   PRINT ''''Success''''  
					ELSE  
					   PRINT ''''Failure''''; 

					EXEC sp_configure ''''show advanced options'''', ''''1''''
					RECONFIGURE
					-- this enables xp_cmdshell
					EXEC sp_configure ''''xp_cmdshell'''', ''''0'''' 
					RECONFIGURE
					'', 
							@database_name=N''master'', 
							@flags=0
					GO
					USE [msdb]
					GO
					EXEC msdb.dbo.sp_update_job @job_name=N''SecurityAudit_Merge_Cleanup'', 
							@enabled=1, 
							@start_step_id=1, 
							@notify_level_eventlog=0, 
							@notify_level_email=2, 
							@notify_level_page=2, 
							@delete_level=0, 
							@description=N'''', 
							@category_name=N''Database Maintenance'', 
							@owner_login_name='+@jobOwner+', 
							@notify_email_operator_name=N'''', 
							@notify_page_operator_name=N''''
					GO
					USE [msdb]
					GO
					DECLARE @schedule_id int
					EXEC msdb.dbo.sp_add_jobschedule @job_name=N''SecurityAudit_Merge_Cleanup'', @name=N''daily'', 
							@enabled=1, 
							@freq_type=4, 
							@freq_interval=1, 
							@freq_subday_type=1, 
							@freq_subday_interval=0, 
							@freq_relative_interval=0, 
							@freq_recurrence_factor=1, 
							@active_start_date=20220424, 
							@active_end_date=99991231, 
							@active_start_time=0, 
							@active_end_time=235959, @schedule_id = @schedule_id OUTPUT
					select @schedule_id
					GO'

				PRINT @jobCMD_step1
				PRINT @jobCMD_step2
end

else --if rollback intended by setting @deploy = 0
begin
	set @servercmd = '
						USE [master]
						GO

						ALTER SERVER AUDIT [audit]
						with (STATE = OFF)
	
						USE [master]
						GO

						/****** Object:  Audit [Audit]    Script Date: 4/24/2022 3:05:58 AM ******/
						DROP SERVER AUDIT [Audit]
						GO

						'
	DECLARE db_cursor2 CURSOR FOR 
	SELECT d.name 
	FROM @databases d join sys.sysdatabases sd on d.name = sd.name and d.name not in (select name from @exceptiondatabases)

	OPEN db_cursor2  
	FETCH NEXT FROM db_cursor2 INTO @name  

	WHILE @@FETCH_STATUS = 0  
	BEGIN  
	set @databaseCMD = '
				USE ['+@name+']

					GO

					ALTER DATABASE AUDIT SPECIFICATION [DatabaseAuditSpecification]
					with (STATE = OFF)

					GO

				USE ['+@name+']
					GO
					DROP DATABASE AUDIT SPECIFICATION [DatabaseAuditSpecification]
					'
		
			Print @databaseCMD
		  FETCH NEXT FROM db_cursor2 INTO @name 
	END 

	CLOSE db_cursor2  
	DEALLOCATE db_cursor2 
	print @serverCMD
	set @audittableCMD = 'USE ['+@mgmtdb+']

	GO
	DROP TABLE [dbo].[audittable]'

	PRINT @audittableCMD

	set @jobCMD_step1 = '

		USE [msdb]
		GO

		/****** Object:  Job [SecurityAudit_Merge_Cleanup]    Script Date: 4/24/2022 3:41:05 AM ******/
		EXEC msdb.dbo.sp_delete_job @job_name = ''SecurityAudit_Merge_Cleanup'', @delete_unused_schedule=1
		GO
		'
		print @jobCMD_step1
		print '
		EXEC sp_configure ''show advanced options'', ''1''
					RECONFIGURE
					-- this enables xp_cmdshell
					EXEC sp_configure ''xp_cmdshell'', ''1'' 
					RECONFIGURE

					DECLARE @result int;  
					EXEC @result = xp_cmdshell ''cd '+@auditfilepath+' & for /f "eol=: delims=" %F in (''''dir /b /o-d /a-d *.sqlaudit'''') do @del "%F"'';  
					IF (@result = 0)  
					   PRINT ''Success''
					ELSE  
					   PRINT ''Failure''; 

					EXEC sp_configure ''show advanced options'', ''1''
					RECONFIGURE
					-- this enables xp_cmdshell
					EXEC sp_configure ''xp_cmdshell'', ''0'' 
					RECONFIGURE'
					
		
end





