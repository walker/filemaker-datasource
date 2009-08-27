<?php 
/** 
 * FileMaker
 * @author Walker Hamilton
 * @date 08/25/2009
 * 
 * Copyright (c) 2009 Walker Hamilton, WalkerHamilton.com
 * 
 *
 * Licensed under The MIT License (see included LICENSE.txt)
 */ 


// =================================================================================
// = FileMaker.php : required base class
// =================================================================================
// FileMaker comes with your FileMaker Pro Server
// Tested with version: 9
// Web Site: www.filemaker.com
// =================================================================================

App::import('Vendor', 'FileMaker', array('file' => 'FileMaker.php'));

class DboFileMaker extends DataSource { 

	var $description = "FileMaker Data Source"; 

	var $_baseConfig = array ( 
		'host' => '192.168.2.153',
		'port' => 80,
	); 

	/**
	 * FileMaker column definition
	 *
	 * @var array
	 */
	var $columns = array(
		'primary_key' => array('name' => 'NUMBER'),
		'string' => array('name' => 'TEXT'),
		'text' => array('name' => 'TEXT'),
		'integer' => array('name' => 'NUMBER','formatter' => 'intval'),
		'float' => array('name' => 'NUMBER', 'formatter' => 'floatval'),
		'datetime' => array('name' => 'TIMESTAMP', 'format' => 'm/d/Y H:i:s', 'formatter' => 'date'),
		'timestamp' => array('name' => 'TIMESTAMP', 'format' => 'm/d/Y H:i:s', 'formatter' => 'date'),
		'time' => array('name' => 'TIME', 'format' => 'H:i:s', 'formatter' => 'date'),
		'date' => array('name' => 'DATE', 'format' => 'm/d/Y', 'formatter' => 'date'),
		'binary' => array('name' => 'CONTAINER'),
		'boolean' => array('name' => 'NUMBER')
	);
	
	/** 
	 * Constructor 
	 */
	function __construct($config = null) {
		$this->debug = Configure :: read() > 0;
		$this->fullDebug = Configure :: read() > 1;
		$this->timeFlag = getMicrotime();
		
		parent :: __construct($config);
		return $this->connect();
	}

	/** 
	 * Destructor. Closes connection to the database. 
	 */ 
	function __destruct() {
		$this->close();
		parent :: __destruct();
	}

	/** 
	 * Connect. Creates connection handler to database 
	 */
	function connect() {
		$this->connected = true;
		
		return $this->connected;
	}

	/** 
	 * Close.
	 */ 
	function close() {
		if ($this->fullDebug && Configure :: read() > 1) {
			$this->showLog();
		}
		$this->disconnect();
	}

	function disconnect() {
		$this->connected = false;
		return $this->connected;
	} 

	/** 
	 * Checks if it's connected to the database 
	 * 
	 * @return boolean True if the database is connected, else false 
	 */ 
	function isConnected() {
		return $this->connected;
	}

	/** 
	 * Reconnects to database server with optional new settings 
	 * 
	 * @param array $config An array defining the new configuration settings 
	 * @return boolean True on success, false on failure 
	 */ 
	function reconnect($config = null) {
		$this->disconnect();
		if ($config != null) {
			$this->config = am($this->_baseConfig, $this->config, $config);
		}
		return $this->connect();
	} 

	/** 
	 * Returns properly formatted field name
	 * 
	 * @param array $config An array defining the new configuration settings 
	 * @return boolean True on success, false on failure 
	 */ 
	function name($data) {
		return $data;
	}

	/*
		TODO_ABG: needs to use recursion
		TODO_ABG: needs to handle filemakers ability to put mutliple tables on one layout
		TODO_ABG: should somehow include the ability to specify layout
	*/
    /** 
     * The "R" in CRUD 
     * 
     * @param Model $model 
     * @param array $queryData 
     * @param integer $recursive Number of levels of association 
     * @return unknown 
     */ 
    function read(&$model, $queryData = array (), $recursive = null) {
		$fm_layout = $model->defaultLayout;
		$fm_database = $model->fmDatabaseName;
		
		// set basic connection data
		if(!isset($this->fm))
			$this->fm =& new Filemaker($fm_database, $this->config['host'], $this->config['login'], $this->config['password']);
		
		// get layout info
		if(!empty($queryData['conditions'])) {
			$findCommand =& $this->fm->newFindCommand($fm_layout);
		} else {
			$findCommand =& $this->fm->newFindAllCommand($fm_layout);
		}
		
		if(!empty($queryData['conditions'])) {
			$conditions = array(); // a clean set of queries
			$isOr = false; // a boolean indicating whether this query is logical OR
			
			foreach($queryData['conditions'] as $conditionField => $conditionValue) {
				// if a logical or statement has been pased somewhere
				if($conditionField == 'or') {
					$isOr = true;
					if(is_array($conditionValue)) {
						$conditions = array_merge($conditions, $conditionValue);
					}
				} else {
					$conditions[$conditionField] = $conditionValue;
				}
			}
			
			foreach($conditions as $conditionField => $conditionValue) {
				$string = $conditionField;
				if(strpos($string,'.')) {
					$stringExp = explode('.', $string);
					unset($stringExp[0]);
					$plainField = implode('.',$stringExp);
				} else {
					$plainField = $string;
				}
				
				$findCommand->addFindCriterion($plainField, $conditionValue);
				
				//add or operator
				if($isOr)
					$findCommand->setLogicalOperator(FILEMAKER_FIND_OR);
				else
					$findCommand->setLogicalOperator(FILEMAKER_FIND_AND);
			}
		}
		
		// set sort order
		foreach($queryData['order'] as $orderCondition) {
			if(!empty($orderCondition)){
				$i = 0;
				foreach($orderCondition as $field => $sortRule) {
					$string = $field;
					$pattern = '/(\w+)\.(-*\w+)$/i';
					$replacement = '${2}';
					$plainField = preg_replace($pattern, $replacement, $string);
					
					$sortRuleFm = (strtolower($sortRule) == 'desc') ? FILEMAKER_SORT_DESCEND : FILEMAKER_SORT_ASCEND;
					
					$findCommand->addSortRule($plainField, 1, $sortRuleFm);
					$i++;
				}
			}
		}
		
		// set skip records if there is an offset
		if(!empty($queryData['limit']) && !empty($queryData['offset'])) {
			$findCommand->setRange($queryData['offset'], $queryData['limit']);
		} else if(!empty($queryData['offset'])) {
			$findCommand->setRange($queryData['offset']);
		} else if(!empty($queryData['limit'])) {
			$findCommand->setRange(500, $queryData['limit']);
		}
		
		// return a found count if requested
		if($queryData['fields'] == 'COUNT') {
			// perform find without returning result data
			$fmResults = $findCommand->execute();
			
			// test result
			if(!$this->handleFMResult($fmResults, $model->name, 'read (count)')) {
				return FALSE;
			}
			
			$countResult = array();
			$countResult[0][0] = array('count' => $fmResults->getFetchCount());
			
			// return found count
			return $countResult;
		} else {
			// perform the find in FileMaker
			$fmResults = $findCommand->execute();
			
			if(!$this->handleFMResult($fmResults, $model->name, 'read')) {
				return FALSE;
			}
		}
		
		$fmRecords = $fmResults->getRecords();
		
		$resultsOut = array();
		// format results
		$i = 0;
		foreach($fmRecords as $fmRecord) {
			foreach($fmRecord->getFields() as $field) {
				if($model->getColumnType($field)=='timestamp' || $model->getColumnType($field)=='time' || $model->getColumnType($field)=='date') {
					$the_field_value = $fmRecord->getFieldAsTimestamp($field);
					
					// Value was out of range or not a date/time field
					if(FileMaker::isError($the_field_value)) { $the_field_value = null; }
				} else {
					$the_field_value = $fmRecord->getField($field);
				}
				if(!empty($the_field_value)) {
					$resultsOut[$i][$model->name][$field] = $the_field_value;
				}
			}
			if(isset($resultsOut[$i][$model->name])) {
				$resultsOut[$i][$model->name]['fm_id'] = $fmRecord->getRecordId();
				$resultsOut[$i][$model->name]['fm_mod_id'] = $fmRecord->getModificationId();
			}
			$i++;
		}
		sort($resultsOut);
		
		/*************** Not Implemented ******************/
		
		// ================================
		// = Searching for Related Models =
		// ================================
		// if ($model->recursive > 0) {
		// }
		
		return $resultsOut;
	}
	
	/**
	 * The "D" in CRUD 
	 * can only delete from the recid that is internal to filemaker
	 * We do this by using the deleteAll model method, which lets us pass conditions to the driver
	 * delete statement. This method will only work if the conditions array contains a 'recid' field
	 * and value. Also, must pass cascade value of false with the deleteAll method.
	 *
	 * @param Model $model
	 * @param array $conditions
	 * @return boolean Success
	 */
	function delete(&$model, $conditions = null) {
		$fm_layout = $model->defualtLayout;
		$fm_database = $model->fmDatabaseName;
		
		if(is_null($conditions)) {
			$fileCommand = $this->fm->newDeleteCommand($model->defualtLayout, $model->getId());
			
			// perform deletion
			$return = $fileCommand->execute(TRUE);
			
			return $this->handleFMResult($return, $model->name, 'delete');
		} else {
				// not quite ready here (don't need it right now, so won't build it)
			foreach($conditions as $field => $value) {
				// Query for all IDs that match criteria
				
				// Loop through and delete each record
				// foreach() {
				// 	$fileCommand = $this->fm->newDeleteCommand($model->defualtLayout, $model->getId());
				// 	$return = $fileCommand->execute(TRUE);
				// 	if(!$this->handleFMResult($return, $model->name, 'delete')) {
				// 		return FALSE;
				// 	} else {
				// 		return TRUE;
				// 	}
				// }
			}
		}
	}
	
	/**
	 * @param Model $model
	 * @param array $fields
	 * @param array $values
	 * @return boolean Success
	 */
	function create(&$model, $fields = null, $values = null) {
		return false; // not implemented
	}
	
	
	/**
	 * @param Model $model
	 * @param array $fields
	 * @param array $values
	 * @param mixed $conditions
	 * @return array
	 */
	function update(&$model, $fields = array(), $values = null, $conditions = null) {
		return false; // not implemented
	}
	
	/**
	 * Returns an array of the fields in given table name.
	 *
	 * @param string $model the model to inspect
	 * @return array Fields in table. Keys are name and type
	 */
	function describe(&$model) {
		// describe caching
		$cache = $this->__describeFromCache($model);
		if ($cache != null) {
			return $cache;
		}
		
		$fm_layout = $model->defaultLayout;
		$fm_database = $model->fmDatabaseName;
		
		// set basic connection data
		$this->fm =& new Filemaker($fm_database, $this->config['host'], $this->config['login'], $this->config['password']);
		
		// get layout info
		$result = $this->fm->getLayout($fm_layout);
		
		$fieldsOut = array();
		
		$fmFieldTypeConversion = array(
			'text' => 'string',
			'date' => 'date',
			'time' => 'time',
			'timestamp' => 'timestamp',
			'number' => 'float',
			'container' => 'binary'
		);
		
		
		foreach($result->getFields() as $field_name => $field_info) {
			$type = $fmFieldTypeConversion[$field_info->getResult()];
			$fieldsOut[$field_info->getName()] = array(
				'type' => $type,
				'null' => null,
				'default' => null,
				'length' => null,
				'key' => null
			);
		}
		
		$this->__cacheDescription($this->fullTableName($model, false), $fieldsOut);
		return $fieldsOut;
	}
	
	/**
	 * __describeFromCache
	 * looks for and potentially returns the cached description of the model
	 * 
	 * @param $model
	 * @return the models cache description or null if none exists
	 */
	function __describeFromCache($model) {
		
		if ($this->cacheSources === false) {
			return null;
		}
		if (isset($this->__descriptions[$model->tablePrefix . $model->table])) {
			return $this->__descriptions[$model->tablePrefix . $model->table];
		}
		$cache = $this->__cacheDescription($model->tablePrefix . $model->table);

		if ($cache !== null) {
			$this->__descriptions[$model->tablePrefix . $model->table] =& $cache;
			return $cache;
		}
		return null;
	}
	
	/**
	 * __cacheDescription
	 * 
	 * @param string $object : name of model
	 * @param mixed $data : the data to be cached
	 * @return mixed : the cached data
	 */
	function __cacheDescription($object, $data = null) {
		if ($this->cacheSources === false) {
			return null;
		}
		
		if ($data !== null) {
			$this->__descriptions[$object] =& $data;
		}
		
		$key = ConnectionManager::getSourceName($this) . '_' . $object;
		$cache = Cache::read($key, '_cake_model_');
		
		
		if (empty($cache)) {
			$cache = $data;
			Cache::write($key, $cache, '_cake_model_');
		}
		
		return $cache;
	}


    /**
     * GenerateAssociationQuery
     */    
	function generateAssociationQuery(& $model, & $linkModel, $type, $association = null, $assocData = array (), & $queryData, $external = false, & $resultSet) { 
		return null; // not implemented
	} 

	/**
	 * QueryAssociation
	 * 
	 */
	function queryAssociation(& $model, & $linkModel, $type, $association, $assocData, & $queryData, $external = false, & $resultSet, $recursive, $stack) { 
		return false; // not implemented
	}

	/** 
     * readAssociated
     * very similar to read but for related data
     * unlike read does not make a reference to the passed model
     * 
     * @param Model $model 
     * @param array $queryData 
     * @param integer $recursive Number of levels of association 
     * @return unknown 
     */ 
	function readAssociated($linkedModel, $queryData = array (), $recursive = null) { 
		return false; // not implemented
    }

	/**
	 * Gets full table name including prefix
	 *
	 * @param mixed $model
	 * @param boolean $quote
	 * @return string Full quoted table name
	 */
	function fullTableName($model, $quote = true) {
		if (is_object($model)) {
			$table = $model->tablePrefix . $model->table;
		} elseif (isset($this->config['prefix'])) {
			$table = $this->config['prefix'] . strval($model);
		} else {
			$table = strval($model);
		}
		if ($quote) {
			return $this->name($table);
		}
		return $table;
	}

	/** 
	 * Returns a formatted error message from previous database operation. 
	 * 
	 * @return string Error message with error number 
	 */ 
	function lastError() {
		// if (FX::isError($this->lastFXError)) {
		// 	return $this->lastFXError.getCode() . ': ' . $this->lastFXError.getMessage();
		// }
		return null;
	}

	/**
	 * handleFXResult
	 * 
	 * logs queries, logs errors, and returns false on error
	 * 
	 * @param FX result object or FX error object
	 * @param string : model name
	 * @param string : action name
	 * 
	 * @return false if result is an FX error object
	 */
	function handleFMResult($result, $modelName = 'N/A', $actionName = 'N/A') {
		$this->_queriesCnt++;
		
		// if a connection error
		if(FileMaker::isError($result)) {
			// log error
			$this->_queriesLog[] = array(
				'model' 	=> $modelName,
				'action' 	=> $actionName,
				'query' 	=> '',
				'error'		=> $result->getMessage(),
				'numRows'	=> '',
				'took'		=> round((getMicrotime() - $this->timeFlag) * 1000, 0)
			);
			if (count($this->_queriesLog) > $this->_queriesLogMax) {
				array_pop($this->_queriesLog);
			}
			
			$this->timeFlag = getMicrotime();
			return FALSE;
		} else {
			// log query
			$this->_queriesLog[] = array(
				'model' 	=> $modelName,
				'action' 	=> $actionName,
				'query' 	=> '', // substr($result['URL'],strrpos($result['URL'], '?')),
				'error'		=> '', // how to 
				'numRows'	=> $result->getFetchCount(),
				'took'		=> round((getMicrotime() - $this->timeFlag) * 1000, 0)
			);
			
			$this->timeFlag = getMicrotime();
			return TRUE;
		}
	}

	/** 
	 * Returns number of rows in previous resultset. If no previous resultset exists, 
	 * this returns false. 
	 * NOT USED
	 * 
	 * @return int Number of rows in resultset 
	 */
	function lastNumRows() {
		return null; 
	}

	/** 
	 * NOT USED
	 */
	function execute($query) {
		return null;
	}
	
	/** 
	 * NOT USED 
	 */
	function fetchAll($query, $cache = true) {
		return array();
	}
	
	// Logs -------------------------------------------------------------- 
	/** 
	 * logQuery
	 */
	function logQuery($query) {
		
	}
	
	/** 
	 * Outputs the contents of the queries log.
	 * 
	 * @param boolean $sorted 
	 */
	function showLog() {
		
		$log = $this->_queriesLog;
		
		$totalTime = 0;
		foreach($log as $entry) {
			$totalTime += $entry['took'];
		}
		
		
		
		if ($this->_queriesCnt > 1) {
			$text = 'queries';
		} else {
			$text = 'query';
		}
		
		if (PHP_SAPI != 'cli') {
			print ("<table class=\"cake-sql-log\" id=\"cakeSqlLog_" . preg_replace('/[^A-Za-z0-9_]/', '_', uniqid(time(), true)) . "\" summary=\"Cake SQL Log\" cellspacing=\"0\" border = \"0\">\n<caption>({$this->configKeyName}) {$this->_queriesCnt} {$text} took {$totalTime} ms</caption>\n");
			print ("<thead>\n<tr><th>Nr</th><th>Model</th><th>Action</th><th>Error</th><th>Num. rows</th><th>Took (ms)</th></tr>\n</thead>\n<tbody>\n");
			
			foreach ($log as $k => $i) {
				print ("<tr><td>" . ($k + 1) . "</td><td>{$i['model']}</td><td>{$i['action']}</td><td>{$i['error']}</td><td style = \"text-align: right\">{$i['numRows']}</td><td style = \"text-align: right\">{$i['took']}</td></tr>\n");
			}
			print ("</tbody></table>\n");
			
		} else {
			foreach ($log as $k => $i) {
				print (($k + 1) . ". {$i['query']} {$i['error']}\n");
			}
		}
	}

	/** 
	 * Output information about a query
	 * NOT USED
	 * 
	 * @param string $query Query to show information on. 
	 */
	function showQuery($query) { 
		
	} 

} 
?>