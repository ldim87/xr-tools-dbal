<?php
/**
 * @author Dmitriy Lukin <lukin.d87@gmail.com>
 */

namespace XrTools;

/**
 * PDO Mysql Adapter for \XrTools\DatabaseManager Interface
 */
class DBMysqlAdapter implements DatabaseManager {

	private $connection;

	private $connectionParams;

	private $lastAffectedRows = 0;

	private $lastInsertId = 0;

	function __construct(array $connectionParams = null){
		// connection settings
		if(isset($connectionParams)){
			$this->setConnectionParams($connectionParams);
		}
	}

	public function validateSettings(array $settings){
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
		$settings['charset'] = $settings['charset'] ?? 'utf8';

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
		
		return $this->lastInsertId ? $this->lastInsertId : true;
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

		return $result->rowCount() ? $result->fetch(\PDO::FETCH_NUM)[0] : '';
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

		return $result->rowCount() ? $result->fetch(\PDO::FETCH_ASSOC) : [];
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

		return $result->rowCount() ? $result->fetchAll(\PDO::FETCH_ASSOC) : [];
	}
	
	/**
	 * Getting data and the number of all rows
	 *
	 * @param string     $query
	 * @param array|null $params
	 * @param array      $opt
	 *
	 * @return array
	 */
	public function fetchArrayWithCount(string $query, array $params = null, array $opt = []){
		return [
			'count' => $this->fetchColumn($this->getExtractCountSQL($query), $params),
			'items' => $this->fetchArray($query, $params) ,
		];
	}

	public function connect(array $settings){
		// validate settings
		$settings = $this->validateSettings($settings);

		// connect
		$connection = new \PDO(
			'mysql:host='.$settings['host'].';dbname='.$settings['dbname'].';charset='.$settings['charset'],
			$settings['username'],
			$settings['password'],
			[
				\PDO::ATTR_EMULATE_PREPARES => false,
				\PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION
			]
		);

		// get handler
		return $connection;
	}

	public function getConnection(){
		// connect if not connected
		if(!isset($this->connection)){
			$this->connection = $this->connect($this->connectionParams);
		}

		return $this->connection;
	}
	
	/**
	 * MySQL transaction start
	 */
	public function start()
	{
		// get connection
		$db = $this->getConnection();
		
		$db->beginTransaction();
	}
	
	/**
	 * MySQL transaction rollback
	 */
	public function rollback()
	{
		// get connection
		$db = $this->getConnection();
		
		$db->rollBack();
	}
	
	/**
	 * MySQL transaction commit
	 */
	public function commit()
	{
		// get connection
		$db = $this->getConnection();
		
		$db->commit();
	}
	
	/**
	 * Get count sql query
	 *
	 * @param $mainQuery
	 *
	 * @return mixed
	 */
	protected function getExtractCountSQL($mainQuery){
		// Remove secondary requests for a while if there are any
		[$query, $attach] = $this->getCountSQLNested($mainQuery);
		
		// Remove from query "order by" and "limit"
		$query = trim(
			preg_replace('/(ORDER BY|LIMIT).*$/is', '', $query)
		);
		
		// Find out if data is being grouped
		preg_match('/(GROUP BY)/is', $query, $preg_gb);
		
		// Depending on the type of getting the number of rows, we get it
		if(empty($preg_gb)){
			$query = preg_replace('/SELECT.*FROM/is', 'SELECT COUNT(*) FROM', $query);
		}else{
			// $query = preg_replace('/SELECT.*FROM/is', 'SELECT * FROM', $query);
			$query = 'SELECT COUNT(*) FROM (' . $query . ') AS `tmp_count`';
		}
		
		// Returning secondary requests to the site
		return $this->setCountSQLNested($query, $attach);
	}
	
	/**
	 * Parse attachments
	 *
	 * @param $query
	 *
	 * @return array
	 */
	protected function getCountSQLNested($query){
		$attach = [];
		$i      = 1;
		
		$query = preg_replace_callback(
			'/\((.*?)\)/is',
			function($matches) use (&$attach, &$i){
				$key          = '#prc' . $i++ . '#';
				$attach[$key] = $matches[1];
				
				return '(' . $key . ')';
			},
			$query
		);
		
		return [$query, $attach];
	}
	
	/**
	 * Collect attachments
	 *
	 * @param $query
	 * @param $attach
	 *
	 * @return mixed
	 */
	protected function setCountSQLNested($query, $attach){
		if(empty($attach)){
			return $query;
		}
		
		return str_replace(
			array_keys($attach),
			array_values($attach),
			$query
		);
	}
	
	

}
