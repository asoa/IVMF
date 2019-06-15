$FILE_PATH = 'H:\pshell_test_import.csv'
$SQL_SERVER = 'VETS-RESEARCH04'
$DebugPreference = 'Continue'  # change to SilentlyContinue to remove debugging messages
$LOG_FILE_SUCCESS = 'H:\ETL_log_success' + (Get-Date).ToString("yyyy-MM-dd-HHmm") + '.log'
$LOG_FILE_FAILURE = 'H:\ETL_log_failure' + (Get-Date).ToString("yyyy-MM-dd-HHmm") + '.log'

# TODO: import lookup_load script

Function check_record_exist {
    # sends query to db to check if $id exists
    Param ([string] $id)
    $sqlcmd = @"
        USE SERVES
        IF EXISTS (SELECT Service_Episode_ID FROM se_episode WHERE service_episode_id = '$id')
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
        # [Parameter(ValueFromPipeline=$true)] [psobject]$rows
    )

    Begin {  # import source csv into csv object and create array of all (including duplicate) records from Episodes table 
        $rows = Import-Csv -Path $FILE_PATH
        $sql_object = Invoke-Sqlcmd -ServerInstance $SQL_SERVER -Query "USE SERVES; SELECT service_episode_id FROM se_episode" 
        $service_episode_ids = @($sql_object | ForEach-Object {$_.Service_Episode_ID})
        
    }

    Process {
        $rows | ForEach-Object  {  # iterate over earch record in csv object
            
            $exist = check_record_exist $_.Service_Episode_ID  # check to see if service_episode_id already exists in db
            if ($exist -eq 1) {
                 # record has not changed, continue to next record
                if (($_.originalnetwork -eq $_.currentnetwork) -and ($_.Current_Organization -eq $_.Originating_Organization)) {
                    $str = 'already in database and network nor organization has changed'
                    Write-Debug ("{0}: {1}" -f $_.Service_Episode_ID,'already in database and neither network nor organization changed')
                    $log_string = ('{0} {1} {2}: {3}' -f $_.Service_Episode_ID,$_.cleaned2ob,$_.flag_clientntwk,'Neither network nor org changed')
                    write_log $log_string 1
                }
                # network changed, organization same -> insert new network record
                elseif (($_.originalnetwork -ne $_.currentnetwork) -and ($_.Current_Organization -eq $_.Originating_Organization)) {
                    $str = 'network changed, inserting new network record'
                    Write-Debug ("{0}: {1}" -f $_.Service_Episode_ID,$str)
                    $log_string = ('{0} {1} {2}: {3}' -f $_.Service_Episode_ID,$_.cleaned2ob,$_.flag_clientntwk,'Updated network')
                    write_log $log_string 0
                }
                # network same, organization diff -> insert new organization record
                elseif (($_.originalnetwork -eq $_.currentnetwork) -and ($_.Current_Organization -ne $_.Originating_Organization)) {
                    $str = 'organization changed, inserting new organization record'
                    Write-Debug ("{0}: {1}" -f $_.Service_Episode_ID,$str)
                    $log_string = ('{0} {1} {2}: {3}' -f $_.Service_Episode_ID,$_.cleaned2ob,$_.flag_clientntwk,'Updated org')
                    write_log $log_string 0
                }
                # network and organization diff -> insert new network and org records
                elseif (($_.originalnetwork -ne $_.currentnetwork) -and ($_.Current_Organization -ne $_.Originating_Organization)) {
                    $str = 'network and organization changed, inserting new network and organization record'
                    Write-Debug ("{0}: {1}" -f $_.Service_Episode_ID,$str)
                    $log_string = ('{0} {1} {2}: {3}' -f $_.Service_Episode_ID,$_.cleaned2ob,$_.flag_clientntwk,'Updated network and org')
                    write_log $log_string 0
                }
            }
            elseif ($exist -eq 0) {  # if record isn't in db, insert it
                $episode_insert = @"
                    use SERVES

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

                    Declare @network_episode_id int = NULL
                    Declare @currentnetwork int = '$($_.currentnetwork)'
                    Declare @originalnetwork int = '$($_.originalnetwork)'
                    Declare @network_id int = '$($_.currentnetwork)'

                    Declare @organization_name nvarchar(50) = '$($_.Current_Organization)'
                    Declare @originating_organization nvarchar(50) = '$($_.Originating_Organization)'

                    INSERT INTO se_episode(service_episode_id, client_id, outcomegrouped, service_type_grouped, [source], started_with, ended_with, outcome, resolution, network_scope) 
                    VALUES (@service_episode_id, @client_id, @outcomegrouped, @service_type_grouped, @source, @started_with, @ended_with, @outcome, @resolution, @network_scope) 
                    --SET @SE_ID = scope_identity()
                    GO

                    INSERT INTO se_network_episode(service_episode_id, network_id, originalnetwork) 
                    VALUES(@service_episode_id, @network_id, @originalnetwork)
                    --SET @network_episode_id = scope_identity()
                    GO

                    INSERT INTO se_network_organization(network_id, organization_name, originating_organization)
                    VALUES(@network_id, @organization_name, @originating_organization)
                    GO

"@  # don't indent the `@` or the preceding sql will fail

                Invoke-Sqlcmd -Query $episode_insert -ServerInstance $SQL_SERVER
                $str = 'Wrote to db'
                Write-Debug ("{0}: {1}" -f $str,$_.Service_Episode_ID)
                $log_string = ('{0} {1} {2}: {3}' -f $_.Service_Episode_ID,$_.cleaned2ob,$_.flag_clientntwk,'Inserted to db')
                write_log $log_string 0
            }
           
            else {
                Write_Debug ('{0}: {1}' -f $_.Service_Episode_ID, "Not inserted to db")
                $log_string = ('{0} {1} {2}: {3}' -f $_.Service_Episode_ID,$_.cleaned2ob,$_.flag_clientntwk,'Not inserted to db')
                write_log $log_string 1
            }
        }
    }
} # end update_database


function init_db {
    <#
        .Description
        iterate over each name in table_names array and drop the table
        
        TODO: code works but does not drop tables with fk reference, need to drop fk references first 
    #>

    # get tablenames from db
    $sqlcmd = @"
        USE SERVES
        SELECT table_name [name]
        FROM INFORMATION_SCHEMA.TABLES
        GO
"@  # don't indent the `@` or the preceding sql will fail
    $table_names = Invoke-Sqlcmd -Query $sqlcmd -ServerInstance $SQL_SERVER | ForEach-Object{$_.Item(0)} | Where-Object{$_ -notin 'sysdiagrams'}

    $table_names | ForEach-Object {
        $drop_table = @"
            USE SERVES
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
    $table_names = @('se_service_subtype','se_service_type','se_network_organization','se_network_episode','se_episode', 'se_network', 'se_organization') 

    $table_names | ForEach-Object {
        $drop_table = @"
            USE SERVES
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
        USE SERVES
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
            [network_scope] NVARCHAR(50)

            CONSTRAINT pk_episodes PRIMARY KEY CLUSTERED (service_episode_id) 
        );
        CREATE TABLE se_network
        (
            [network_id] INT NOT NULL,
            [network_name] VARCHAR(20),
            [network_state] VARCHAR(2),
            [network_region] INT,
            [network_state_enc] INT

            CONSTRAINT pk_network PRIMARY KEY CLUSTERED (network_id)
        );
        CREATE TABLE se_network_episode
        (
            [network_episode_id] INT IDENTITY(1,1) NOT NULL,
            [service_episode_id] NVARCHAR(50) NOT NULL,
            [network_id] INT NULL,
            [originalnetwork] INT

            CONSTRAINT pk_network_episode PRIMARY KEY CLUSTERED (network_episode_id),
            CONSTRAINT pk_service_episode_ID FOREIGN KEY (service_episode_id) REFERENCES se_episode
        );
        CREATE TABLE se_organization
        (
            [organization_name] NVARCHAR(50) NOT NULL,
            [program_id] NVARCHAR(50),
            [program_name] NVARCHAR(50),
            [program_created_at] INT,
            [program_updated_at] INT,
            [service_type_program_provides] NVARCHAR(50)

            CONSTRAINT pk_current_organization_name PRIMARY KEY CLUSTERED (organization_name)
        );
        CREATE TABLE se_network_organization
        (
            [network_organization_id] INT IDENTITY(1,1) NOT NULL,
            [network_id] INT NULL,
            [organization_name] NVARCHAR(50),
            [originating_organization] NVARCHAR(50)

            CONSTRAINT pk_network_organization PRIMARY KEY CLUSTERED (network_organization_id),
            CONSTRAINT fk_network_id FOREIGN KEY (network_id) REFERENCES se_network_episode,
            CONSTRAINT fk_current_organization FOREIGN KEY (organization_name) REFERENCES se_organization
        );
        CREATE TABLE se_service_type
        (
            [service_type_id] INT IDENTITY(1,1) NOT NULL,
            [service_episode_id] NVARCHAR(50),
            [service_type_name] NVARCHAR(50)

            CONSTRAINT pk_service_type PRIMARY KEY CLUSTERED (service_type_id)
            CONSTRAINT fk_service_episode_id FOREIGN KEY(service_episode_id) REFERENCES se_episode
        );
        CREATE TABLE se_service_subtype
        (
            [service_subtype_id] INT IDENTITY(1,1) NOT NULL,
            [service_type_id] INT,
            [service_subtype_name] NVARCHAR(50)

            CONSTRAINT pk_service_subtype_id PRIMARY KEY CLUSTERED (service_type_id)
            CONSTRAINT fk_service_type_id FOREIGN KEY(service_type_id) REFERENCES se_service_type
        );

"@  # don't indent the `@` or the preceding sql will fail

    Invoke-Sqlcmd -Query $create_tables -ServerInstance $SQL_SERVER 
      
    $sqlcmd = @"
        USE SERVES
        SELECT table_name [name]
        FROM INFORMATION_SCHEMA.TABLES
        GO
"@  
    $table_names = Invoke-Sqlcmd -Query $sqlcmd -ServerInstance $SQL_SERVER | ForEach-Object{$_.Item(0)} | Where-Object{$_ -notin 'sysdiagrams'}
    Write-Host ('Created the following tables: ' , $table_names)
                    
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
 



