<?php
/**
 * @author Dmitriy Lukin <lukin.d87@gmail.com>
 */

namespace XrTools;

use \PDO;
use \PDOException;

/**
 * PDO Mysql Adapter for \XrTools\DatabaseManager Interface
 */
class DBMysqlAdapter implements DatabaseManager {

	protected $connection;

	protected $connectionParams;

	protected $lastAffectedRows = 0;

	protected $lastInsertId = 0;

	protected $isTransactionStarted = false;

	function __construct(array $connectionParams = null){
		// connection settings
		if(isset($connectionParams)){
			$this->setConnectionParams($connectionParams);
		}
	}

	protected function validateSettings(array $settings){
		// mandatory settings
		if(
			empty($settings['dbname']) || 
			empty($settings['username']) || 
			empty($settings['password']) || 
			empty($settings['host'])
		){
			throw new \Exception('Invalid connection settings!');
		}

		// optional setting
		$settings['charset'] = $settings['charset'] ?? 'utf8mb4';

		return $settings;
	}

	public function setConnectionParams(array $settings){
		$this->connectionParams = $this->validateSettings($settings);
	}

	public function query(string $query, array $params = null, array $opt = []){
		// get connection
		$db = $this->getConnection();

		// prepared statements
		if($params && is_array($params)){
			$result = $db->prepare($query);
			$result->execute($params);
			$this->lastAffectedRows = $result->rowCount();
		} else {
			$result = $db->exec($query);
			$this->lastAffectedRows = $result;
		}

		$this->lastInsertId = $db->lastInsertId();
		
		return $this->lastInsertId ? (int) $this->lastInsertId : true;
	}

	public function getAffectedRows(){
		// get affected rows from last query
		return $this->lastAffectedRows;
	}

	public function fetchColumn(string $query, array $params = null, array $opt = []){
		// get connection
		$db = $this->getConnection();

		// prepared statements
		if($params && is_array($params)){
			$result = $db->prepare($query);
			$result->execute($params);

		} else {
			$result = $db->query($query);
		}

		return $result->rowCount() ? $result->fetch(PDO::FETCH_NUM)[0] : '';
	}
	
	public function fetchRow(string $query, array $params = null, array $opt = []){
		// get connection
		$db = $this->getConnection();

		// prepared statements
		if($params && is_array($params)){
			$result = $db->prepare($query);
			$result->execute($params);
		} else {
			$result = $db->query($query);
		}

		return $result->rowCount() ? $result->fetch(PDO::FETCH_ASSOC) : [];
	}

	public function fetchArray(string $query, array $params = null, array $opt = []){
		// get connection
		$db = $this->getConnection();

		// prepared statements
		if($params && is_array($params)){
			$result = $db->prepare($query);
			$result->execute($params);
		} else {
			$result = $db->query($query);
		}

		return $result->rowCount() ? $result->fetchAll(PDO::FETCH_ASSOC) : [];
	}

	protected function connect(array $settings){
		// validate settings
		$settings = $this->validateSettings($settings);

		$pdo_settings = [
			PDO::ATTR_EMULATE_PREPARES => false,
			PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
		];

		try {

			// connect
			$connection = new PDO(
				'mysql:host='.$settings['host'].';dbname='.$settings['dbname'].';charset='.$settings['charset'],
				$settings['username'],
				$settings['password'],
				$pdo_settings
			);

			// get handler
			return $connection;
		}
		// resolve broken connection
		catch (PDOException $e) {

			// rethrow connection exception			
			throw new DBConnectionException($e->getMessage());
		}
	}

	protected function getConnection(){
		// connect if not connected
		if(!isset($this->connection)){
			$this->connection = $this->connect($this->connectionParams);
		}

		return $this->connection;
	}

	/**
	 * @param bool $debug
	 * @return bool
	 * @throws \Exception
	 */
	public function start(bool $debug = false): bool
	{
		if ($this->isTransactionStarted) {
			throw new \Exception('Transaction has already started');
		}

		// get connection
		$db = $this->getConnection();

		$res = $db->beginTransaction();

		$this->isTransactionStarted = true;

		return $res;
	}

	/**
	 * @param bool $debug
	 * @return bool
	 */
	public function rollback(bool $debug = false): bool
	{
		// get connection
		$db = $this->getConnection();

		$res = $db->rollBack();

		$this->isTransactionStarted = false;

		return $res;
	}

	/**
	 * @param bool $debug
	 * @return bool
	 */
	public function commit(bool $debug = false): bool
	{
		// get connection
		$db = $this->getConnection();
		
		$res = $db->commit();

		$this->isTransactionStarted = false;

		return $res;
	}
	
	/**
	 * Stub
	 * @return array
	 */
	public function getQueryCollection(){
		return [];
	}

	/**
	 * Insert / Update table via params
	 * @param array  $data       Table data
	 * @param string $table_name Table name
	 * @param mixed  $index      Table update key (id or opt.index_key)
	 * @param array  $opt        Options
	 */
	public function set(array $data, string $table_name, $index = null, array $opt = []){
				
		$query_data = $this->getInsertUpdateQuery($data, $table_name, $index, $opt);

		if(empty($query_data['query'])){
			return ['status' => false, 'message' => 'Empty query!'];
		}

		return $this->query(
			$query_data['query'],
			$query_data['params'],
			[
				'debug' => !empty($opt['debug']),
				'return' => $opt['return'] ?? null
			]
		);
	}

	public function getInsertUpdateQuery(array $data, string $table_name, $index = null, array $opt = []){
		
		$result = [
			'query' => '',
			'params' => []
		];

		// empty data |or  empty table name
		if(!$data || !strlen($table_name)){
			return $result;
		}

		// multiple row input data
		if(isset($data[0])){
			$result = $this->getMultiRowSetQuery($data, $table_name, $opt);
		}
		// single row input data
		else {
			$result = $this->getSingleRowSetQuery($data, $table_name, $index, $opt);
		}

		return $result;
	}

	protected function getSingleRowSetQuery(array $data, string $table_name, $index = null, array $opt = []){

		$result = [
			'query' => '',
			'params' => []
		];

		// create sql query
		$sql = '';
		
		// manual WHERE ($index priority)
		$where = $opt['where'] ?? '';
		$where_vals = !empty($opt['where_vals']) && is_array($opt['where_vals']) ? $opt['where_vals'] : [];
		
		foreach ($data as $key => $value) {
			// add to query
			if($sql) $sql .= ', ';
			
			$sql .= '`'.$key.'`=?';
			$result['params'][] = $value;
		}
		
		// Update by index_key (id)
		if($index){
			$index_key = $opt['index_key'] ?? 'id';
			$where = 'WHERE `'.$index_key.'`=?';
			
			$result['params'][] = $index;
		}
		// Update through manual WHERE
		elseif($where && $where_vals){
			foreach ($where_vals as $key => $val){
				$result['params'][] = $val;
			}
		}

		$insert_update = $where ? 'UPDATE' : 'INSERT';

		$on_duplicate_key_update = !empty($opt['on_duplicate_key_update']) && is_array($opt['on_duplicate_key_update']) && $insert_update == 'INSERT'
			? ' ON DUPLICATE KEY UPDATE ' . implode(', ', array_map(function($item){ return "`{$item}`=VALUES(`{$item}`)"; }, $opt['on_duplicate_key_update']))
			: '';

		// construct query
		$result['query'] = "{$insert_update} `{$table_name}` SET {$sql} {$where}{$on_duplicate_key_update}";

		return $result;
	}

	protected function getMultiRowSetQuery(array $data, string $table_name, array $opt = []){

		$result = [
			'query' => '',
			'params' => []
		];
		
		if(!isset($data[0])){
			return $result;
		}

		// get keys from first row (as template)
		$keys = array_keys($data[0]);

		$on_duplicate_key_update = !empty($opt['on_duplicate_key_update']) && is_array($opt['on_duplicate_key_update'])
			? ' ON DUPLICATE KEY UPDATE ' . implode(', ', array_map(function($item){ return "`{$item}`=VALUES(`{$item}`)"; }, $opt['on_duplicate_key_update']))
			: '';

		$result['query'] = 'INSERT INTO `'.$table_name.'` (`'.implode('`,`', $keys).'`) VALUES ' .
			implode(', ', array_fill(
				0, count($data), '('.implode(',', array_fill(0, count($keys), '?')).')'
			)) . 
			$on_duplicate_key_update;

		$result['params'] = array_merge(...array_map(
			function($item){
				return array_values($item);
			},
			$data
		));

		return $result;
	}
}
