$SQL_SERVER = 'VETS-RESEARCH04'

function load_lookups {
    Import-Csv -Path 'H:\network_table.csv' | ForEach-Object { 
    $sqlcmd = @"
        Declare @network_id int = '$($_.network_id)'
        Declare @network_name varchar(20) = '$($_.network_name)'
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

    Import-Csv -Path 'H:\org_table.csv' | ForEach-Object { 
    $sqlcmd = @"
        Declare @organization_id nvarchar(50) = '$($_.organization_id)'
        Declare @organization_name nvarchar(50) = '$($_.organization_name)'

        USE SERVES
        INSERT INTO se_organization(organization_id, organization_name)
        VALUES(@organization_id, @organization_name)
        GO
"@
    Invoke-Sqlcmd -Query $sqlcmd -ServerInstance $SQL_SERVER   
    }
}

load_lookups

