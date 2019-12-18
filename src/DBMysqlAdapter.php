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
}
