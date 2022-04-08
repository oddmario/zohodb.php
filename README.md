# zohodb.php
A PHP port of ZohoDB.py

## Example usage
```php
<?php
require_once("zohodb.php");

$auth = new ZohoAuthHandler("Zoho client ID here", "Zoho client secret here");
$db = new ZohoDB($auth, array("Spreadsheet1"));
var_dump($db->select([
    "table" => "users",
    "criteria" => '"username" = "mario"'
]));
```
