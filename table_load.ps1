#$FILE_PATH = 'H:\SE_FINAL_20190430_test.csv'
$FILE_PATH = 'H:\pshell_test_import.csv'
$SQL_SERVER = 'VETS-RESEARCH04'
$DebugPreference = 'Continue'  # change to SilentlyContinue to remove debugging messages
$LOG_FILE_SUCCESS = 'H:\ETL_log_success' + (Get-Date).ToString("yyyy-MM-dd-HHmmss") + '.log'
$LOG_FILE_FAILURE = 'H:\ETL_log_failure' + (Get-Date).ToString("yyyy-MM-dd-HHmmss") + '.log'

# TODO: import lookup_load script

Function check_record_exist {
    # sends query to db to check if $id exists
    Param ([string] $id)
    $sqlcmd = @"
        USE SERVES_TEST
        IF EXISTS (SELECT service_episode_id FROM se_episode WHERE service_episode_id = '$id')
        select 1
        ELSE 
        select 0
"@  # don't indent the `@` or the preceding sql will fail
    $result = Invoke-Sqlcmd -Query $sqlcmd -ServerInstance $SQL_SERVER | ForEach-Object {$_.Item(0)}
    return $result
}

function write_log {
   Param (
       [Parameter(Mandatory=$true, Position=0)] 
       [string]$logstring, 
       [Parameter(Mandatory=$true, Position=1)] 
       [int]$num 
   )

   # $stamp = (Get-Date).ToString("yyyy/MM/dd HH:mm:ss")
   # $log_entry = $logstring + " " + $stamp

   if($num -eq 0) {Add-content $LOG_FILE_SUCCESS -value $logstring} 
   else {Add-content $LOG_FILE_FAILURE -value $logstring}
}

function update_database {
    <#
        Checks first to see if episodeid exists, orig/current network diff, orig/current org diff
        case 1: if episodeid does not exist, insert the episode, network, and org record
        case 2: if episodeid exists and network is diff but org is same, update network
        case 3: if episodeid exists and network is same but org is diff, update org
        case 4: if episodeid exists and network is diff and org is diff, update both
        case 5: if episodeid exists network and org are the same, continue
    #>

    Param (
        # [Parameter(ValueFromPipeline=$true)] [psobject]$obj
    )

    Begin {  # import source csv into csv object and create array of all (including duplicate) records from Episodes table 
        $rows = Import-Csv -Path $FILE_PATH 
    }

    Process {
        $rows | ForEach-Object  {  # iterate over each record in csv object
            $last_updated_ = Get-Date $_.Last_Updated -Format 'yyyy-MM-dd hh:mm:ss'  # transform source date string to sql datetime2 format
            $exist = check_record_exist $_.Service_Episode_ID  # check to see if service_episode_id already exists in db
            Write-Host ('{0}: {1}' -f $_.Service_Episode_ID, $exist)
            if ($exist -eq 1) {  # service_episoded_id is already in db, updated the network/org or both
                $sqlcmd = @"
                    USE SERVES_TEST
                    Declare @sp_result INT
                    EXEC check_net_org_same '$($_.Service_Episode_ID)', '$($_.currentnetwork)', '$($_.Current_Organization)', @result = @sp_result OUTPUT
                    select @sp_result
"@
                $result = Invoke-Sqlcmd -Query $sqlcmd -ServerInstance $SQL_SERVER | ForEach-Object {$_.Item(0)}
                # Execute stored procedure to check if input network and org is the same/diff as data in db
                Write-Host ('{0}: Result:{1}' -f $_.Service_Episode_ID, $result)
                if($result -eq 0) {  # record has not changed, continue to next record

                    Write-Host ('Net nor Org has changed, continuing to next record')
                    return

                } elseif($result -eq 1) {  # net changed, org same
                    $sqlcmd = @"
                        USE SERVES_TEST
                        Declare @sp_result INT
                        EXEC update_network '$($_.Service_Episode_ID)', '$($_.currentnetwork)', '$($_.originalnetwork)', '$($_.Current_Organization)', '$($_.Originating_Organization)', '$last_updated_', @result = @sp_result OUTPUT
                        SELECT @sp_result
"@
                    $result = Invoke-Sqlcmd -Query $sqlcmd -ServerInstance $SQL_SERVER | ForEach-Object {$_.Item(0)}
                    Write-Debug ('Attempted to updated network_episode table with return code: {0}' -f $result)
                } elseif($result -eq 2) {  # net same, org changed
                    $sqlcmd = @"
                        USE SERVES_TEST
                        Declare @sp_result INT
                        EXEC update_org '$($_.Service_Episode_ID)', '$($_.Current_Organization)', '$($_.Originating_Organization)', '$last_updated_', @result = @sp_result OUTPUT
                        SELECT @sp_result
"@
                    $result = Invoke-Sqlcmd -Query $sqlcmd -ServerInstance $SQL_SERVER | ForEach-Object {$_.Item(0)}
                    Write-Debug ('Attempted to updated network_organization table with return code: {0}' -f $result)

                } else {  # both net and org changed
                    $sqlcmd = @"
                        USE SERVES_TEST
                        Declare @sp_result INT
                        EXEC update_network_org '$($_.Service_Episode_ID)', '$($_.currentnetwork)', '$($_.originalnetwork)', '$($_.Current_Organization)', '$($_.Originating_Organization)', '$last_updated_', @result = @sp_result OUTPUT
                        SELECT @sp_result
"@
                    $result = Invoke-Sqlcmd -Query $sqlcmd -ServerInstance $SQL_SERVER | ForEach-Object {$_.Item(0)}
                    write-Debug ('Attempted to update network_episode and network_organization tables with return code: {0}' -f $result)
                }

            }
            elseif ($exist -eq 0) {  # if record isn't in db, insert it
                $last_updated_ = Get-Date $_.Last_Updated -Format 'yyyy-MM-dd hh:mm:ss'  # transform source date string to sql datetime2 format
                $episode_insert = @"
                    USE SERVES_TEST
                  
                    Declare @service_episode_id nvarchar(50) = '$($_.Service_Episode_ID)'
                    Declare @client_id nvarchar(50) = '$($_.Client_ID)'
                    Declare @outcomegrouped nvarchar(50) = '$($_.outcomegrouped)'
                    Declare @service_type_grouped nvarchar(50) = '$($_.Service_Type_Grouped)'
                    Declare @source nvarchar(50) = '$($_.Source)'
                    Declare @started_with nvarchar(50) = '$($_.Started_With)'
                    Declare @ended_With nvarchar(50) = '$($_.Ended_With)'
                    Declare @outcome nvarchar(50) = '$($_.Outcome)'
                    Declare @resolution nvarchar(50) = '$($_.Resolution)'
                    Declare @network_scope nvarchar(50) = '$($_.Network_Scope)'
                    Declare @status varchar(20) = '$($_.Status)'

                    Declare @age int = '$($_.Age)'
                    Declare @mil_affiliation varchar(25) = '$($_.Military_Affiliation)'
                    Declare @branch varchar(25) = '$($_.Branch)'
                    Declare @gender varchar(10) = '$($_.Gender)'
                                      
                    Declare @service_type_id int = NULL
                    Declare @service_type_name nvarchar(50) = '$($_.Service_Type)'

                    Declare @service_subtype_name nvarchar(50) = '$($_.Service_Subtype)'
                    
                    INSERT INTO se_episode(service_episode_id, client_id, outcomegrouped, service_type_grouped, [source], started_with, ended_with, outcome, resolution, network_scope, status) 
                    VALUES (@service_episode_id, @client_id, @outcomegrouped, @service_type_grouped, @source, @started_with, @ended_with, @outcome, @resolution, @network_scope, @status) 
                    
                    INSERT INTO se_service_type(service_episode_id, service_type_name)
                    VALUES(@service_episode_id, @service_type_name)
                    SET @service_type_id = scope_identity()

                    INSERT INTO se_service_subtype(service_type_id, service_subtype_name)
                    VALUES(@service_type_id, @service_subtype_name)

                    INSERT INTO se_demographic(service_episode_id, age, mil_affiliation, branch, gender)
                    VALUES(@service_episode_id, @age, @mil_affiliation, @branch, @gender)
"@  
                Invoke-Sqlcmd -Query $episode_insert -ServerInstance $SQL_SERVER
                $episode_date_metrics = @"
                    USE SERVES_TEST

                    --se_episode_date
                    Declare @service_episode_id nvarchar(50) = '$($_.Service_Episode_ID)'
                    Declare @datecreated_y int = '$($_.datecreated_y)'
                    Declare @datecreated_m int = '$($_.datecreated_m)'
                    Declare @datecreated_calqtr int = '$($_.datecreated_calqtr)'
                    Declare @dateclosed_y int = '$($_.dateclosed_y)'
                    Declare @dateclosed_m int = '$($_.dateclosed_m)'
                    Declare @dateclosed_calqtr int = '$($_.dateclosed_calqtr)'
                    Declare @qtrcreated int = '$($_.qtrcreated)'
                    Declare @qtrclosed int = '$($_.qtrclosed)'
                    Declare @last_updated datetime2 = '$last_updated_'

                    --se_episode_metrics
                    Declare @duration_service_episode real = '$($_.Duration_of_Service_Episode__day)'
                    Declare @duration_case_created real = '$($_.Time_from_Start_to_Case_Created)'
                    Declare @duration_case_closed real = '$($_.Duration_of_Case__days_)'  
                    Declare @duration_program_entry real = '$($_.Time_from_Start_to_Program_Entry)'
                    Declare @duration_referral_acc real = '$($_.Time_from_CC_Referral_to_Org_Acc)'

                    INSERT INTO se_episode_date(service_episode_id, datecreated_y, datecreated_m, datecreated_calqtr, dateclosed_y, dateclosed_m, dateclosed_calqtr, qtrcreated, qtrclosed,last_updated)
                    VALUES(@service_episode_id, @datecreated_y, @datecreated_m, @datecreated_calqtr, @dateclosed_y, @dateclosed_m, @dateclosed_calqtr, @qtrcreated, @qtrclosed, @last_updated)

                    INSERT INTO se_episode_metrics(service_episode_id,duration_service_episode,duration_case_created,duration_case_closed,duration_program_entry,duration_referral_acc)
                    VALUES(@service_episode_id,@duration_service_episode,@duration_case_created,@duration_case_closed,@duration_program_entry,@duration_referral_acc)

"@
                Invoke-Sqlcmd -Query $episode_date_metrics -ServerInstance $SQL_SERVER

                $current_organization_ = $_.Current_Organization.Replace("'",'"')
                $originating_organization = $_.Originating_Organization.Replace("'",'"')
                $sqlcmd = @"
                    USE SERVES_TEST

                    Declare @network_episode_id int = NULL
                    Declare @service_episode_id nvarchar(50) = '$($_.Service_Episode_ID)'
                    Declare @network_id int = '$($_.currentnetwork)'
                    --Declare @currentnetwork int = '$($_.currentnetwork)'
                    Declare @originalnetwork int = '$($_.originalnetwork)'    
                    Declare @network_organization_id int = NULL 
                    Declare @last_updated datetime2 = '$last_updated_'        

                    INSERT INTO se_network_episode(service_episode_id, network_id, originalnetwork, last_updated) 
                    VALUES(@service_episode_id, @network_id, @originalnetwork, @last_updated)
                    SET @network_episode_id = scope_identity()

                    SET @network_organization_id = @network_episode_id
                    
                    Declare @organization_name nvarchar(100) = '$($current_organization_)'
                    Declare @originating_organization nvarchar(100) = '$($originating_organization)'

                    INSERT INTO se_network_organization(network_episode_id, organization_name, originating_organization, last_updated)
                    VALUES(@network_episode_id, @organization_name, @originating_organization, @last_updated)
"@
                Invoke-Sqlcmd -Query $sqlcmd -ServerInstance $SQL_SERVER

                Write-Debug ("{0}: {1}" -f $_.Service_Episode_ID, 'Successful INSERT')
                $log_string = ('{0} {1} {2}: {3}' -f $_.Service_Episode_ID,$_.cleaned2ob,$_.flag_clientntwk,'Inserted to db')
                write_log $log_string 0
            }
           
            else {
                Write-Debug ('{0}: {1}' -f $_.Service_Episode_ID, "Not inserted to db")
                $log_string = ('{0} {1} {2}: {3}' -f $_.Service_Episode_ID,$_.cleaned2ob,$_.flag_clientntwk,'Not inserted to db')
                write_log $log_string 1
            }
        }
    
    }
    
} # end load_database


function init_db {
    <#
        .Description
        iterate over each name in table_names array and drop the table
        
        TODO: code works but does not drop tables with fk reference, need to drop fk references first 
    #>

    # get tablenames from db
    $sqlcmd = @"
        USE SERVES_TEST
        SELECT table_name [name]
        FROM INFORMATION_SCHEMA.TABLES
        GO
"@  # don't indent the `@` or the preceding sql will fail
    $table_names = Invoke-Sqlcmd -Query $sqlcmd -ServerInstance $SQL_SERVER | ForEach-Object{$_.Item(0)} | Where-Object{$_ -notin 'sysdiagrams'}

    $table_names | ForEach-Object {
        $drop_table = @"
            USE SERVES_TEST
            GO
            IF OBJECT_ID('$_', 'U') IS NOT NULL
            DROP TABLE $_
            GO
"@
    Invoke-Sqlcmd -Query $drop_table -ServerInstance $SQL_SERVER
    Write-Host ("Dropped table: {0}" -f $_)
    }
}  

function drop_tables {
    $table_names = @('se_episode_date','se_episode_metrics','se_service_subtype','se_service_type','se_network_organization','se_organization','se_network_episode','se_demographic','se_episode','se_network') 

    $table_names | ForEach-Object {
        $drop_table = @"
            USE SERVES_TEST
            GO
            IF OBJECT_ID('$_', 'U') IS NOT NULL
            DROP TABLE $_
            GO
"@  # don't indent the `@` or the preceding sql will fail
        Invoke-Sqlcmd -Query $drop_table -ServerInstance $SQL_SERVER
        Write-Host ("Dropped table: {0}" -f $_)
    }
}


function create_tables {
    $create_tables = @"        
        USE SERVES_TEST
        CREATE TABLE se_episode
        (
            [service_episode_id] NVARCHAR(50) NOT NULL,
            [client_id] NVARCHAR(50) NULL,
            [outcomegrouped] NVARCHAR(50),
            [service_type_grouped] NVARCHAR(50),
            [source] NVARCHAR(50),
            [started_with] NVARCHAR(50),
            [ended_with] NVARCHAR(50),
            [outcome] NVARCHAR(50),
            [resolution] NVARCHAR(50),
            [network_scope] NVARCHAR(50),
            [status] VARCHAR(20)

            CONSTRAINT pk_episode PRIMARY KEY CLUSTERED (service_episode_id) 
        );
        CREATE TABLE se_episode_date
        (
            --[episode_date_id] INT IDENTITY(1,1) NOT NULL,
            [service_episode_id] NVARCHAR(50) NOT NULL,
            [datecreated_y] INT,
            [datecreated_m] INT,
            [datecreated_calqtr] INT,
            [dateclosed_y] INT,
            [dateclosed_m] INT,
            [dateclosed_calqtr] INT,
            [qtrcreated] INT,
            [qtrclosed] INT,
            [last_updated] DATETIME2

            CONSTRAINT pk_episode_date PRIMARY KEY CLUSTERED (service_episode_id),
            CONSTRAINT fk_episode_date FOREIGN KEY (service_episode_id) REFERENCES se_episode
        );
        CREATE TABLE se_episode_metrics
        (
            --[episode_metrics_id] INT IDENTITY(1,1) NOT NULL,
            [service_episode_id] NVARCHAR(50) NOT NULL,
            [duration_service_episode] FLOAT(24),
            [duration_case_created] FLOAT(24),
            [duration_case_closed] FLOAT(24),
            [duration_program_entry] FLOAT(24),
            [duration_referral_acc] FLOAT(24),

            CONSTRAINT pk_episode_metrics PRIMARY KEY CLUSTERED (service_episode_id),
            CONSTRAINT fk_episode_metrics FOREIGN KEY (service_episode_id) REFERENCES se_episode
        );
        CREATE TABLE se_network
        (
            [network_id] INT NOT NULL,
            [network_name] VARCHAR(20),
            [network_state] VARCHAR(2),
            [network_region] INT,
            [network_state_enc] INT,

            CONSTRAINT pk_network PRIMARY KEY CLUSTERED (network_id)
        );
        CREATE TABLE se_network_episode
        (
            [network_episode_id] INT IDENTITY(1,1) NOT NULL,
            [service_episode_id] NVARCHAR(50) NOT NULL,
            [network_id] INT NOT NULL,
            [originalnetwork] INT,
            [last_updated] DATETIME2

            CONSTRAINT pk_network_episode PRIMARY KEY CLUSTERED (network_episode_id),
            CONSTRAINT fk_network_episode1 FOREIGN KEY (service_episode_id) REFERENCES se_episode,
            CONSTRAINT fk_network_episode2 FOREIGN KEY (network_id) REFERENCES se_network
        );
        CREATE TABLE se_organization
        (
            [organization_name] NVARCHAR(100) NOT NULL,
            [organization_id] NVARCHAR(50),

            CONSTRAINT pk_organization PRIMARY KEY CLUSTERED (organization_name)
        );
        CREATE TABLE se_network_organization
        (
            [network_organization_id] INT IDENTITY(1,1) NOT NULL,
            [network_episode_id] INT NOT NULL,
            [organization_name] NVARCHAR(100) NULL,
            [originating_organization] NVARCHAR(100) NULL,
            [last_updated] DATETIME2

            CONSTRAINT pk_network_organization PRIMARY KEY CLUSTERED (network_organization_id),
            CONSTRAINT fk_network_organization1 FOREIGN KEY (network_episode_id) REFERENCES se_network_episode,
            CONSTRAINT fk_network_organization2 FOREIGN KEY (organization_name) REFERENCES se_organization
        );
        CREATE TABLE se_service_type
        (
            [service_type_id] INT IDENTITY(1,1) NOT NULL,
            [service_episode_id] NVARCHAR(50),
            [service_type_name] NVARCHAR(50),

            CONSTRAINT pk_service_type PRIMARY KEY CLUSTERED (service_type_id),
            CONSTRAINT fk_service_type FOREIGN KEY(service_episode_id) REFERENCES se_episode
        );
        CREATE TABLE se_service_subtype
        (
            [service_subtype_id] INT IDENTITY(1,1) NOT NULL,
            [service_type_id] INT,
            [service_subtype_name] NVARCHAR(50),

            CONSTRAINT pk_service_subtype PRIMARY KEY CLUSTERED (service_subtype_id),
            CONSTRAINT fk_service_subtype FOREIGN KEY(service_type_id) REFERENCES se_service_type
        );
        CREATE TABLE se_demographic
        (
            --[demographic_id] INT IDENTITY(1,1) NOT NULL,
            [service_episode_id] NVARCHAR(50) UNIQUE NOT NULL,
            [age] INT NULL,
            [mil_affiliation] VARCHAR(25) NULL,
            [branch] VARCHAR(25) NULL,
            [gender] VARCHAR(10) NULL,

            --CONSTRAINT pk_demographic PRIMARY KEY CLUSTERED (demographic_id),
            CONSTRAINT fk_demographic FOREIGN KEY(service_episode_id) REFERENCES se_episode
        );

"@  # don't indent the `@` or the preceding sql will fail

    Invoke-Sqlcmd -Query $create_tables -ServerInstance $SQL_SERVER 
      
    $sqlcmd = @"
        USE SERVES_TEST
        SELECT table_name [name]
        FROM INFORMATION_SCHEMA.TABLES
        GO
"@  
    $table_names = Invoke-Sqlcmd -Query $sqlcmd -ServerInstance $SQL_SERVER | ForEach-Object{$_.Item(0)} | Where-Object{$_ -notin 'sysdiagrams'}
    Write-Host 'Created the following tables:'
    #Write-Host ('Created the following tables: ' , $table_names)
    $table_names | ForEach-Object {Write-Host $_}
                    
}


function generate_report {
    Write-Host 'Generating Report'
    #TODO
}

function main {
[int] $prompt = Read-Host -Prompt @"
    Select from one of the items below:
        1. Update Database
        2. Drop Tables
        3. Create tables
        4. Generate Report 'a'
        
        >>
"@
    if ($prompt -eq 1) {update_database}
    elseif ($prompt -eq 2) {drop_tables}
    elseif ($prompt -eq 3) {create_tables}
    else {generate_report}   
}

main
