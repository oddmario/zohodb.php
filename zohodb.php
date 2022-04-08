<?php
$ZOHO_OAUTH_API_BASE = "https://accounts.zoho.com/oauth/v2";
$ZOHO_SHEETS_API_BASE = "https://sheet.zoho.com/api/v2";

class EmptyInput extends \Exception { }
class InvalidType extends \Exception { }
class UnexpectedResponse extends \Exception { }
class MissingData extends \Exception { }
class InvalidCacheTable extends \Exception { }

class ZohoDBRequests {
    
    function request($url, $is_post = 0, $headers = array(), $data = array()) {
        $ch = curl_init($url);
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);
        if( $is_post >= 1 ) {
            curl_setopt($ch, CURLOPT_POST, 1);
            curl_setopt($ch, CURLOPT_POSTFIELDS, $data);
        }
        curl_setopt($ch, CURLOPT_HTTPHEADER, $headers);
        curl_setopt($ch, CURLOPT_FOLLOWLOCATION, 1);
        $res = curl_exec($ch);
        curl_close($ch);
        return $res;
    }
    
    function parallel_requests($urls, $max_conns = 24, $is_post = 0, $headers = array(), $data = array()) {
        $urls_count = count($urls);
        $curl_array = array();
        $chm = curl_multi_init();
        curl_multi_setopt($chm, CURLMOPT_MAXCONNECTS, $max_conns);
        for($i = 0; $i < $urls_count; $i++) {
            $url = $urls[$i];
            $curl_array[$i] = curl_init($url);
            curl_setopt($curl_array[$i], CURLOPT_RETURNTRANSFER, 1);
            if( $is_post >= 1 ) {
                curl_setopt($curl_array[$i], CURLOPT_POST, 1);
                curl_setopt($curl_array[$i], CURLOPT_POSTFIELDS, $data);
            }
            curl_setopt($curl_array[$i], CURLOPT_HTTPHEADER, $headers);
            curl_setopt($curl_array[$i], CURLOPT_FOLLOWLOCATION, 1);
            curl_multi_add_handle($chm, $curl_array[$i]);
        }
        do {
            curl_multi_exec($chm, $running);
        } while($running > 0);
        for($i = 0; $i < $urls_count; $i++) {
            $results[] = curl_multi_getcontent($curl_array[$i]);
        }
        curl_multi_close($chm);
        return $results;
    }
    
}

class ZohoDBCache {
    
    private $hash;
    private $cache_path;
    
    function __construct($hash) {
        if( empty($hash) ) {
            throw new MissingData("The cache hash is required");
        }
        $this->hash = $hash;
        $this->cache_path = "./.zohodb/db_cache/$this->hash";
        if( !is_dir($this->cache_path) ) {
            mkdir($this->cache_path, 0777, true);
        }
    }
    
    private function wait_till_released($table) {
        while(1) {
            if( file_exists("$this->cache_path/$table.lock") ) {
                sleep(1);
                continue;
            } else {
                break;
            }
        }
        return true;
    }
    
    private function lock($table) {
        file_put_contents("$this->cache_path/$table.lock", "");
        return true;
    }
    
    private function release($table) {
        if( file_exists("$this->cache_path/$table.lock") ) {
            unlink("$this->cache_path/$table.lock");
        }
        return true;
    }
    
    private function release_and_return($return_value, $table) {
        $this->release($table);
        return $return_value;
    }
    
    public function set($table, $key, $value) {
        $this->wait_till_released($table);
        $this->lock($table);
        if( !file_exists("$this->cache_path/$table.json") ) {
            $data = array();
            $data[$key] = $value;
            file_put_contents("$this->cache_path/$table.json", json_encode($data));
            return $this->release_and_return(true, $table);
        }
        $currently_cached = json_decode(file_get_contents("$this->cache_path/$table.json"), true);
        $currently_cached[$key] = $value;
        file_put_contents("$this->cache_path/$table.json", json_encode($currently_cached));
        return $this->release_and_return(true, $table);
    }
    
    public function get($table, $key) {
        if( !file_exists("$this->cache_path/$table.json") ) {
            throw new InvalidCacheTable();
        }
        $currently_cached = json_decode(file_get_contents("$this->cache_path/$table.json"), true);
        if( !isset($currently_cached[$key]) ) {
            return NULL;
        } else {
            return $currently_cached[$key];
        }
    }
    
    public function delete($table, $key) {
        if( !file_exists("$this->cache_path/$table.json") ) {
            throw new InvalidCacheTable();
        }
        $this->wait_till_released($table);
        $this->lock($table);
        $currently_cached = json_decode(file_get_contents("$this->cache_path/$table.json"), true);
        if( !isset($currently_cached[$key]) ) {
            return $this->release_and_return(false, $table);
        } else {
            unset($currently_cached[$key]);
            file_put_contents("$this->cache_path/$table.json", json_encode($currently_cached));
            return $this->release_and_return(true, $table);
        }
    }
    
}

class ZohoAuthHandler {
    public $client_id;
    public $client_secret;
    private $hash;
    private $cache_path;
    private $rh;
    
    function __construct($client_id, $client_secret) {
        if( empty($client_id) || empty($client_secret) ) {
            throw new MissingData('Missing the Zoho authentication credentials');
        }
        $this->client_id = $client_id;
        $this->client_secret = $client_secret;
        $this->hash = md5($this->client_id . ":" . $this->client_secret);
        $this->cache_path = "./.zohodb/auth_cache/$this->hash";
        if( !is_dir($this->cache_path) ) {
            mkdir($this->cache_path, 0777, true);
        }
        $this->rh = new ZohoDBRequests();
    }
    
    private function fetch_token($redirected_url = "") {
        global $ZOHO_OAUTH_API_BASE;
        $redirecturi = urlencode("https://example.com");
        $request_code_params = array(
            "response_type=code",
            "client_id=$this->client_id",
            "scope=ZohoSheet.dataAPI.UPDATE,ZohoSheet.dataAPI.READ",
            "redirect_uri=$redirecturi",
            "access_type=offline",
            "prompt=consent"
        );
        $to_visit = "$ZOHO_OAUTH_API_BASE/auth?" . join("&", $request_code_params);
        if( $redirected_url == "" ) {
            header("Content-Type: text/html");
            echo "Please visit <a href='$to_visit' target='_blank'>this URL</a>";
            echo "<br /><br />Pass the URL, you've been redirected to after authorizing the app, to the token() function (be fast here before the code expires)";
            die();
        }
        $url = trim($redirected_url);
        $urlparams = explode("&", explode("/", $url)[3]);
        $code = "";
        foreach( $urlparams as $param ) {
            $param = explode("=", $param);
            $key = $param[0];
            $val = $param[1];
            if( $key == "code" || $key == "?code" ) {
                $code = $val;
                break;
            }
        }
        $request_token_params = array(
            "code=$code",
            "client_id=$this->client_id",
            "client_secret=$this->client_secret",
            "redirect_uri=$redirecturi",
            "grant_type=authorization_code"
        );
        $ts = time();
        $tokenreq = $this->rh->request("$ZOHO_OAUTH_API_BASE/token?" . join("&", $request_token_params), 1);
        $tokenres = json_decode($tokenreq, true);
        if( !isset($tokenres['access_token']) ) {
            throw new UnexpectedResponse("Failed to obtain an access token");
        }
        $tokenres['created_at'] = $ts;
        file_put_contents("$this->cache_path/token.json", json_encode($tokenres));
        return $tokenres['access_token'];
    }
    
    private function refresh_token($refresh_token) {
        global $ZOHO_OAUTH_API_BASE;
        $req_params = array(
            "client_id=$this->client_id",
            "client_secret=$this->client_secret",
            "refresh_token=$refresh_token",
            "grant_type=refresh_token"
        );
        $ts = time();
        $tokenreq = $this->rh->request("$ZOHO_OAUTH_API_BASE/token?" . join("&", $req_params), 1);
        $tokenres = json_decode($tokenreq, true);
        if( !isset($tokenres['access_token']) ) {
            throw new UnexpectedResponse("Failed to obtain an access token");
        }
        $currently_cached = json_decode(file_get_contents("$this->cache_path/token.json"), true);
        $currently_cached['access_token'] = $tokenres['access_token'];
        $currently_cached['created_at'] = $ts;
        $currently_cached['expires_in'] = $tokenres['expires_in'];
        file_put_contents("$this->cache_path/token.json", json_encode($currently_cached));
        return $tokenres['access_token'];
    }
    
    public function token($redirected_url = "") {
        if( !file_exists("$this->cache_path/token.json") ) {
            file_put_contents("$this->cache_path/token.json", "{}");
        }
        $token_data = json_decode(file_get_contents("$this->cache_path/token.json"), true);
        if( !isset($token_data['access_token']) || !isset($token_data['refresh_token']) || !isset($token_data['expires_in']) || !isset($token_data['created_at']) ) {
            return $this->fetch_token($redirected_url);
        }
        if( (intval($token_data['created_at']) + intval($token_data['expires_in'])) <= time() ) {
            return $this->refresh_token($token_data['refresh_token']);
        }
        return $token_data['access_token'];
    }
    
}

class ZohoDB {
    
    private $AuthHandler;
    private $workbooks;
    private $max_threads;
    private $hash;
    private $cache;
    private $rh;
    
    function __construct($AuthHandler, $workbooks, $max_threads = 24) {
        if( !($AuthHandler instanceof ZohoAuthHandler) ) {
            throw new InvalidType("Invalid ZohoAuthHandler instance passed");
        }
        if( empty($workbooks) ) {
            throw new EmptyInput("Couldn't find any workbook names to use");
        }
        $this->AuthHandler = $AuthHandler;
        $this->workbooks = $workbooks;
        $this->max_threads = intval($max_threads);
        $this->hash = md5("['" . join("', '", $workbooks) . "']");
        $this->cache = new ZohoDBCache($this->hash);
        $this->rh = new ZohoDBRequests();
    }
    
    private function fetch_workbooks() {
        global $ZOHO_SHEETS_API_BASE;
        $workbookids = array();
        $req = $this->rh->request("$ZOHO_SHEETS_API_BASE/workbooks?method=workbook.list", 0, array(
            "Authorization: Bearer " . $this->AuthHandler->token()
        ));
        $res = json_decode($req, true);
        if( $res['status'] == "failure" ) {
            throw new UnexpectedResponse($res['error_message']);
        }
        foreach($res['workbooks'] as $workbook) {
            if( in_array($workbook['workbook_name'], $this->workbooks) ) {
                array_push($workbookids, $workbook['resource_id']);
            }
        }
        if( empty($workbookids) ) {
            throw new UnexpectedResponse("Unable to find any workbooks with the name(s) specified");
        }
        $this->cache->set("workbooks", "workbooks", $workbookids);
        return $workbookids;
    }
    
    public function workbookids() {
        try {
            $wbs = $this->cache->get("workbooks", "workbooks");
        } catch (InvalidCacheTable $e) {
            return $this->fetch_workbooks();
        }
        if($wbs == NULL || empty($wbs)) {
            return $this->fetch_workbooks();
        }
        return $wbs;
    }
    
    public function escape($criteria, $parameters) {
        foreach($parameters as $k => $v) {
            $k = trim($k);
            $v = str_replace("\"", "'", $v);
            $criteria = str_replace($k, $v, $criteria);
        }
        return $criteria;
    }
    
    public function select($opts = array()) {
        global $ZOHO_SHEETS_API_BASE;
        $requireds = array(
            "table",
            "criteria"
        );
        foreach($requireds as $required) {
            if( !isset($opts[$required]) ) {
                throw new MissingData("Missing the required argument '$required'");
            }
        }
        $table = strval($opts['table']);
        $criteria = strval($opts['criteria']);
        if( !in_array("columns", $opts) ) {
            $columns = array();
        } else {
            $columns = $opts['columns'];
        }
        $returned = array();
        $workbookids = $this->workbookids();
        $urls = array();
        foreach( $workbookids as $workbook ) {
            array_push($urls, "$ZOHO_SHEETS_API_BASE/$workbook");
        }
        $responses = $this->rh->parallel_requests($urls, $this->max_threads, 1, array(
            "Authorization: Bearer " . $this->AuthHandler->token()
        ), http_build_query(array(
            "method" => "worksheet.records.fetch",
            "worksheet_name" => $table,
            "criteria" => $criteria,
            "column_names" => join(",", $columns)
        )));
        foreach($responses as $index => $res) {
            $res = json_decode($res, true);
            if( $res['status'] == "failure" ) {
                throw new UnexpectedResponse($res['error_message']);
            }
            foreach($res['records'] as $k => $v) {
                $res['records'][$k]['workbook_id'] = $workbookids[$index];
            }
            $returned = array_merge($returned, $res['records']);
        }
        return $returned;
    }
    
    public function insert($opts = array()) {
        global $ZOHO_SHEETS_API_BASE;
        $requireds = array(
            "table",
            "data"
        );
        foreach($requireds as $required) {
            if( !isset($opts[$required]) ) {
                throw new MissingData("Missing the required argument '$required'");
            }
        }
        $table = strval($opts['table']);
        $data = $opts['data'];
        $workbookids = $this->workbookids();
        foreach($workbookids as $workbook) {
            try {
                $cached_ts = $this->cache->get("full_workbooks", strval($workbook));
                if( $cached_ts !== NULL ) {
                    if( ($cached_ts + 3600) <= time() ) {
                        $this->cache->delete("full_workbooks", strval($workbook));
                    } else {
                        break;
                    }
                }
            } catch(InvalidCacheTable $e) {}
            $req = $this->rh->request("$ZOHO_SHEETS_API_BASE/$workbook", 1, array(
                "Authorization: Bearer " . $this->AuthHandler->token()
            ), http_build_query(array(
                "method" => "worksheet.records.add",
                "worksheet_name" => $table,
                "json_data" => json_encode($data)
            )));
            $res = json_decode($req, true);
            if( isset($res['error_message']) ) {
                if( in_array($res['error_message'], array(2870, 2872)) ) {
                    $this->cache->set("full_workbooks", strval($workbook), time());
                    continue;
                }
            }
            if( $res['status'] == "failure" ) {
                throw new UnexpectedResponse($res['error_message']);
            }
            return true;
        }
        return false;
    }
    
    public function update($opts = array()) {
        global $ZOHO_SHEETS_API_BASE;
        $requireds = array(
            "table",
            "criteria",
            "data"
        );
        foreach($requireds as $required) {
            if( !isset($opts[$required]) ) {
                throw new MissingData("Missing the required argument '$required'");
            }
        }
        $table = strval($opts['table']);
        $criteria = strval($opts['criteria']);
        $data = $opts['data'];
        if( isset($opts['workbook_id']) ) {
            $workbook_id = trim(strval($opts['workbook_id']));
        } else {
            $workbook_id = "";
        }
        $return_bool = false;
        if( $workbook_id !== "" ) {
            $req = $this->rh->request("$ZOHO_SHEETS_API_BASE/$workbook_id", 1, array(
                "Authorization: Bearer " . $this->AuthHandler->token()
            ), http_build_query(array(
                "method" => "worksheet.records.update",
                "worksheet_name" => $table,
                "criteria" => $criteria,
                "data" => json_encode($data)
            )));
            $res = json_decode($req, true);
            if( $res['status'] == "failure" ) {
                throw new UnexpectedResponse($res['error_message']);
            }
            if( $res['no_of_affected_rows'] >= 1 ) {
                $return_bool = true;
            }
        } else {
            $workbookids = $this->workbookids();
            $urls = array();
            foreach( $workbookids as $workbook ) {
                array_push($urls, "$ZOHO_SHEETS_API_BASE/$workbook");
            }
            $responses = $this->rh->parallel_requests($urls, $this->max_threads, 1, array(
                "Authorization: Bearer " . $this->AuthHandler->token()
            ), http_build_query(array(
                "method" => "worksheet.records.update",
                "worksheet_name" => $table,
                "criteria" => $criteria,
                "data" => json_encode($data)
            )));
            foreach($responses as $res) {
                $res = json_decode($res, true);
                if( $res['status'] == "failure" ) {
                    throw new UnexpectedResponse($res['error_message']);
                }
                if( $res['no_of_affected_rows'] >= 1 ) {
                    $return_bool = true;
                }
            }
        }
        return $return_bool;
    }
    
    public function delete($opts = array()) {
        global $ZOHO_SHEETS_API_BASE;
        $requireds = array(
            "table",
            "criteria"
        );
        foreach($requireds as $required) {
            if( !isset($opts[$required]) ) {
                throw new MissingData("Missing the required argument '$required'");
            }
        }
        $table = strval($opts['table']);
        $criteria = strval($opts['criteria']);
        if( isset($opts['workbook_id']) ) {
            $workbook_id = trim(strval($opts['workbook_id']));
        } else {
            $workbook_id = "";
        }
        if( isset($opts['row_id']) ) {
            $row_id = intval($opts['row_id']);
        } else {
            $row_id = 0;
        }
        if( $row_id > 0 ) {
            $rowid = json_encode(array($row_id));
        } else {
            $rowid = "";
        }
        $return_bool = false;
        $affected_workbooks = array();
        if( $workbook_id !== "" ) {
            $req = $this->rh->request("$ZOHO_SHEETS_API_BASE/$workbook_id", 1, array(
                "Authorization: Bearer " . $this->AuthHandler->token()
            ), http_build_query(array(
                "method" => "worksheet.records.delete",
                "worksheet_name" => $table,
                "criteria" => $criteria,
                "row_array" => $rowid,
                "delete_rows" => "true"
            )));
            $res = json_decode($req, true);
            if( $res['status'] == "failure" ) {
                throw new UnexpectedResponse($res['error_message']);
            }
            if( $res['no_of_rows_deleted'] >= 1 ) {
                if( !in_array($workbook_id, $affected_workbooks) ) {
                    array_push($affected_workbooks, $workbook_id);
                }
                $return_bool = true;
            }
        } else {
            $workbookids = $this->workbookids();
            $urls = array();
            foreach( $workbookids as $workbook ) {
                array_push($urls, "$ZOHO_SHEETS_API_BASE/$workbook");
            }
            $responses = $this->rh->parallel_requests($urls, $this->max_threads, 1, array(
                "Authorization: Bearer " . $this->AuthHandler->token()
            ), http_build_query(array(
                "method" => "worksheet.records.delete",
                "worksheet_name" => $table,
                "criteria" => $criteria,
                "row_array" => $rowid,
                "delete_rows" => "true"
            )));
            foreach($responses as $index => $res) {
                $res = json_decode($res, true);
                if( $res['status'] == "failure" ) {
                    throw new UnexpectedResponse($res['error_message']);
                }
                if( $res['no_of_rows_deleted'] >= 1 ) {
                    if( !in_array($workbookids[$index], $affected_workbooks) ) {
                        array_push($affected_workbooks, $workbookids[$index]);
                    }
                    $return_bool = true;
                }
            }
        }
        if( $return_bool == true ) {
            foreach( $affected_workbooks as $workbook ) {
                try {
                    $this->cache->delete("full_workbooks", strval($workbook));
                } catch(InvalidCacheTable $e) {}
            }
        }
        return $return_bool;
    }
    
}
