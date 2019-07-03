$SQL_SERVER = 'localhost'
$PATH = 'C:\Users\vagrant\Documents'

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

        USE SERVES
        INSERT INTO se_network(network_id, network_name, network_state, network_region, network_state_enc)
        VALUES(@network_id, @network_name, @network_state, @network_region, @network_state_enc)
        GO
"@
    Invoke-Sqlcmd -Query $sqlcmd -ServerInstance $SQL_SERVER
    }
    Write-Host "Loaded se_network table"
    
    # load org table
    $path_ = '{0}\org_table.csv' -f $PATH
    Import-Csv -Path $path_ | ForEach-Object { 
    $organization_name_ = $_.organization_name.Replace("'",'"')
    $organization_county_ = $_.organization_county.Replace("'",'"')
    $sqlcmd = @"
        Declare @organization_name nvarchar(100) = '$($organization_name_)'
        Declare @organization_city varchar(50) = '$($_.organization_city)'
        Declare @organization_state varchar(2) = '$($_.organization_state)'
        Declare @organization_zipcode varchar(5) = '$($_.organization_postal_code)'
        Declare @organization_county varchar(30) = '$($organization_county_)'

        USE SERVES
        INSERT INTO se_organization(organization_name, organization_city, organization_state, organization_zipcode, organization_county)
        VALUES(@organization_name, @organization_city, @organization_state, @organization_zipcode, @organization_county)
        GO
"@
    Invoke-Sqlcmd -Query $sqlcmd -ServerInstance $SQL_SERVER   
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

        USE SERVES
        INSERT INTO se_program(program_id, organization_name, program_name)
        VALUES(@program_id, @organization_name, @program_name)
        GO
"@
    Invoke-Sqlcmd -Query $sqlcmd -ServerInstance $SQL_SERVER   
    }
    Write-Host "Loaded se_program table"

    
    # load program_service table

    $path_ = '{0}\program_service_table.csv' -f $PATH
    Import-Csv -Path $path_ | ForEach-Object { 
    
    $sqlcmd = @"
        Declare @program_id nvarchar(50) = '$($_.program_id)'
        Declare @program_service nvarchar(100) = '$($_.service_type_program_provides)'

        USE SERVES
        INSERT INTO se_program_service(program_id, service_type)
        VALUES(@program_id, @program_service)
        GO
"@
    Invoke-Sqlcmd -Query $sqlcmd -ServerInstance $SQL_SERVER   
    }
    Write-Host "Loaded se_program_service table"
    
}

load_lookups
