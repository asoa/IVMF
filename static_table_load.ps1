$SQL_SERVER = 'localhost'
$PATH = 'C:\Users\vagrant\Documents'
$LOG_FILE_FAILURE = '{0}\lookup_log_failure_{1}.log' -f $PATH, (Get-Date).ToString("yyyy-MM-dd-HHmmss")

function write_log {
   Param (
       [Parameter(Mandatory=$true, Position=0)] 
       [string]$logstring, 
       [Parameter(Mandatory=$true, Position=1)] 
       [int]$num,
       [Parameter(Mandatory=$true, Position=2)] 
       [int]$row_num 
   )

   $log_entry =  "line {0}: " -f $row_num + $logstring

   if($num -eq 0) {Add-content $LOG_FILE_SUCCESS -value $log_entry} 
   else {Add-content $LOG_FILE_FAILURE -value $log_entry}
}

function load_lookups {
    # load network table
    
    $path_ = '{0}\network_table.csv' -f $PATH
    Import-Csv -Path $path_ | ForEach-Object { 
    $sqlcmd = @"
        Declare @network_id int = '$($_.network_id)'
        Declare @network_name varchar(50) = '$($_.network_name)'
        Declare @network_state varchar(2) = '$($_.network_state)'
        Declare @network_region int = '$($_.network_region)'
        Declare @network_state_enc int = '$($_.network_state_enc)'

        USE SERVES2
        INSERT INTO se_network(network_id, network_name, network_state, network_region, network_state_enc)
        VALUES(@network_id, @network_name, @network_state, @network_region, @network_state_enc)
        GO
"@
    $sql_error = $null
    Invoke-Sqlcmd -Query $sqlcmd -ServerInstance $SQL_SERVER -ErrorVariable sql_error -ErrorAction SilentlyContinue
    }
    Write-Host "Loaded se_network table"
    
    # load org table
    $path_ = '{0}\org_table.csv' -f $PATH
    $row_number = 1
    Import-Csv -Path $path_ | ForEach-Object { 
    $row_number++
    try {
        $organization_name_ = $_.organization_name.Replace("'",'"')
        $organization_county_ = $_.organization_county.Replace("'",'"')
    }
    catch {
        write_log "transformation error" 1 $row_number
    }
    
    $sqlcmd = @"
        Declare @organization_name nvarchar(100) = '$($organization_name_)'
        Declare @organization_city varchar(50) = '$($_.organization_city)'
        Declare @organization_state varchar(2) = '$($_.organization_state)'
        Declare @organization_zipcode varchar(5) = '$($_.organization_postal_code)'
        Declare @organization_county varchar(30) = '$($organization_county_)'

        USE SERVES2
        INSERT INTO se_organization(organization_name, organization_city, organization_state, organization_zipcode, organization_county)
        VALUES(@organization_name, @organization_city, @organization_state, @organization_zipcode, @organization_county)
        GO
"@
    $sql_error = $null
    Invoke-Sqlcmd -Query $sqlcmd -ServerInstance $SQL_SERVER -ErrorVariable sql_error -ErrorAction SilentlyContinue
    if($sql_error) {write_log $sql_error 1 $row_number}
    }
    Write-Host "Loaded se_organization table"

    # load program table
    $path_ = '{0}\program_table.csv' -f $PATH
    Import-Csv -Path $path_ | ForEach-Object { 
    $organization_name_ = $_.organization_name.Replace("'",'"')
    $program_name_ = $_.program_name.Replace("'",'"')
    $sqlcmd = @"
        Declare @program_id nvarchar(50) = '$($_.program_id)'
        Declare @organization_name nvarchar(100) = '$($organization_name_)'
        Declare @program_name nvarchar(100) = '$($program_name_)'

        USE SERVES2
        INSERT INTO se_program(program_id, organization_name, program_name)
        VALUES(@program_id, @organization_name, @program_name)
        GO
"@
    $sql_error = $null
    Invoke-Sqlcmd -Query $sqlcmd -ServerInstance $SQL_SERVER -ErrorVariable sql_error -ErrorAction SilentlyContinue   
    }
    Write-Host "Loaded se_program table"

    
    # load program_service table

    $path_ = '{0}\program_service_table.csv' -f $PATH
    Import-Csv -Path $path_ | ForEach-Object { 
    
    $sqlcmd = @"
        Declare @program_id nvarchar(50) = '$($_.program_id)'
        Declare @program_service nvarchar(100) = '$($_.service_type_program_provides)'

        USE SERVES2
        INSERT INTO se_program_service(program_id, service_type)
        VALUES(@program_id, @program_service)
        GO
"@
    $sql_error = $null
    Invoke-Sqlcmd -Query $sqlcmd -ServerInstance $SQL_SERVER -ErrorVariable sql_error -ErrorAction SilentlyContinue 
    }
    Write-Host "Loaded se_program_service table"
    
}

load_lookups
