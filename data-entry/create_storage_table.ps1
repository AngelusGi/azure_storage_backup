Install-Module AzTable -Scope CurrentUser

Import-Module Az.Accounts
Import-Module Az.Storage
Import-Module AzTable

# Variables
$resourceGroup = "<resource-group-name>"
$storageAccount = "<storage-account-name>"
$tableName = "<table-name>" # expected to be already created

Write-Host "Connecting to Azure..."
Connect-AzAccount -UseDeviceAuthentication

# Get the storage account context
Write-Host "Getting storage account..."
$ctx = (Get-AzStorageAccount -ResourceGroupName $resourceGroup -Name $storageAccount).Context

# Create the table if it does not exist
New-AzStorageTable -Name $tableName -Context $ctx -ErrorAction SilentlyContinue

Write-Host "Getting table..."
$storageTable = Get-AzStorageTable -Name $tableName -Context $ctx

$cloudTable = $storageTable.CloudTable

Write-Host "Data entry in progress..."
# Row 1 (users/1)
Add-AzTableRow -Table $cloudTable -PartitionKey "users" -RowKey ("1") -Property @{
    Name        = "Alice";
    Age         = 30;
    Balance     = 1050.75;
    LargeNumber = 9876543210L;
    IsActive    = $true;
    SignupDate  = (Get-Date "2025-08-20T10:15:00Z");
    UserId      = [guid]"550e8400-e29b-41d4-a716-446655440000";
    UserName    = [System.Convert]::FromBase64String("QWxpY2UzMAo=")  # sample UserName in base64 '<Name><Age>'
}

# Row 2 (users/2)
Add-AzTableRow -Table $cloudTable -PartitionKey "users" -RowKey ("2") -Property @{
    Name        = "Bob";
    Age         = 45;
    Balance     = 2099.99;
    LargeNumber = 12345678901234L;
    IsActive    = $false;
    SignupDate  = (Get-Date "2025-08-21T14:30:00Z");
    UserId      = [guid]"123e4567-e89b-12d3-a456-426614174000";
    UserName    = [System.Convert]::FromBase64String("Qm9iNDUK")  # sample UserName in base64 '<Name><Age>'
}

# Row 3 (admins/1)
Add-AzTableRow -Table $cloudTable -PartitionKey "admins" -RowKey ("1") -Property @{
    Name        = "Charlie";
    Age         = 38;
    Balance     = 9999.50;
    LargeNumber = 888888888888L;
    IsActive    = $true;
    SignupDate  = (Get-Date "2025-08-22T08:45:00Z");
    UserId      = [guid]"9a7d8c6e-4f3b-4f72-94f8-9f92b123abcd";
    UserName    = [System.Convert]::FromBase64String("Q2hhcmxpZTM4Cg==")  # sample UserName in base64 '<Name><Age>'
}

