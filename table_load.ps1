$FILE_PATH = 'H:\pshell_test_import.csv'
$SQL_SERVER = 'VETS-RESEARCH04'
$DebugPreference = 'Continue'  # change to SilentlyContinue to remove debugging messages

Function check_record_exist {
    # sends query to db to check if $id exists
    Param ([string] $id)
$sqlcmd = @"
    USE SERVES
    IF EXISTS (SELECT Service_Episode_ID FROM Episodes WHERE Service_Episode_ID = '$id')
    select 1
    ELSE 
    select 0
"@
    $result = Invoke-Sqlcmd -Query $sqlcmd -ServerInstance $SQL_SERVER | ForEach-Object {$_.Item(0)}
    return $result
}

Function update_database {
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
        $sql_object = Invoke-Sqlcmd -ServerInstance $SQL_SERVER -Query "USE SERVES; SELECT Service_Episode_ID FROM Episodes" 
        $service_episode_ids = @($sql_object | ForEach-Object {$_.Service_Episode_ID})
        
    }

    Process {
        $rows | ForEach-Object  {  # iterate over earch record in csv object
            
            $exist = check_record_exist $_.Service_Episode_ID  # check to see if service_episode_id already exists in db
            if ($exist -eq 1) {
                 # record has not changed, continue to next record
                if (($_.originalnetwork -eq $_.currentnetwork) -and ($_.Current_Organization -eq $_.Originating_Organization)) {
                    $str = 'already in database and network nor organization has changed'
                    Write-Debug ("{0}: {1}" -f $_.Service_Episode_ID,'already in database and network nor organization has changed')
                }
                # network changed, organization same -> insert new network record
                elseif (($_.originalnetwork -ne $_.currentnetwork) -and ($_.Current_Organization -eq $_.Originating_Organization)) {
                    $str = 'network changed, inserting new network record'
                    Write-Debug ("{0}: {1}" -f $_.Service_Episode_ID,$str)
                }
                # network same, organization diff -> insert new organization record
                elseif (($_.originalnetwork -eq $_.currentnetwork) -and ($_.Current_Organization -ne $_.Originating_Organization)) {
                    $str = 'organization changed, inserting new organization record'
                    Write-Debug ("{0}: {1}" -f $_.Service_Episode_ID,$str)
                }
            }
            elseif ($exist -eq 0) {  # if record isn't in db, insert it
$episode_insert = @"
    use SERVES

    Declare @SE_ID int = NULL
    Declare @Service_Episode_ID nvarchar(50) = '$($_.Service_Episode_ID)'
    Declare @Client_ID nvarchar(50) = '$($_.Client_ID)'
    Declare @outcomegrouped nvarchar(50) = '$($_.outcomegrouped)'
    Declare @Service_Type_Grouped nvarchar(50) = '$($_.Service_Type_Grouped)'
    Declare @Source nvarchar(50) = '$($_.Source)'
    Declare @Started_With nvarchar(50) = '$($_.Started_With)'
    Declare @Ended_With nvarchar(50) = '$($_.Ended_With)'
    Declare @Outcome nvarchar(50) = '$($_.Outcome)'
    Declare @Resolution nvarchar(50) = '$($_.Resolution)'
    Declare @Network_Scope nvarchar(50) = '$($_.Network_Scope)'

    Declare @Network_Episode_ID int = NULL
    Declare @currentnetwork int = '$($_.currentnetwork)'
    Declare @originalnetwork int = '$($_.originalnetwork)'
    Declare @Network_ID int = '$($_.currentnetwork)'

    INSERT INTO Episodes(Service_Episode_ID, Client_ID, outcomegrouped, Service_Type_Grouped, [Source], Started_With, Ended_With, Outcome, Resolution, Network_Scope) 
    VALUES (@Service_Episode_ID, @Client_ID, @outcomegrouped, @Service_Type_Grouped, @Source, @Started_With, @Ended_With, @Outcome, @Resolution, @Network_Scope) 
    SET @SE_ID = scope_identity()
    GO

    INSERT INTO Network_Episode(SE_ID, Network_ID, originalnetwork) 
    VALUES(@SE_ID, @Network_ID, @originalnetwork)
    SET @Network_Episode_ID = scope_identity()
    SELECT @Network_Episode_ID
    GO
"@
                Invoke-Sqlcmd -Query $episode_insert -ServerInstance $SQL_SERVER
                $str = 'Wrote to db'
                Write-Debug ("{0}: {1}" -f $str,$_.Service_Episode_ID)
            }
           
            else {
                Write_Debug ('{0}: {1}' -f $_.Service_Episode_ID, "Not inserted to db")
                # Write-Debug "[Not Written] $_ not written to database"
            }
        }
    }
    
} # end insert_updates


Function init_db {
     Write-Host 'Dropping and re-creating Tables'
     #TODO
}

Function generate_report {
    Write-Host 'Generating Report'
    #TODO
}
 

[int] $prompt = Read-Host -Prompt @"
    Select from one of the items below:
        1. Update Database
        2. Drop and Re-create tables in Database
        3. Generate Report 'a'
        
        >>
"@

# Main control flow
if ($prompt -eq 1) {update_database}
elseif ($prompt -eq 2) {init_db}
else {generate_report}    
