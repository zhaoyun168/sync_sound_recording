<?php
/**
 * MysqlClient
 */
namespace Library;

use Doctrine\DBAL\Configuration;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;

/**
 * Class MysqlClient
 * @package Library
 */
class MysqlClient
{
    /**
     * @var Object
     */
    protected $config;
    /** @var  \Doctrine\DBAL\Connection */
    protected $dbConn;
    /**
     * @var Object
     */
    protected $logger;

    private $tryDBConnectFaildTimes = 0;

    /**
     * MysqlClient constructor.
     * @param $config
     */
    public function __construct($config)
    {
        $this->config=$config;
        $LoggerClient=new LoggerClient($this->config);
        $this->logger=$LoggerClient->getMonolog('mysql-client','log/mysql-client/'.date('Y-m-d').'.log',0);
    }

    /**
     * @return Connection
     * @throws \Doctrine\DBAL\DBALException
     */
    public function getdbConn(){
        if ($this->dbConn instanceof Connection && $this->dbConn->isConnected()) {
            $this->dbConn->close();
        }
        $config = new Configuration();
        $this->dbConn = DriverManager::getConnection($this->config['member-interface'], $config);
        return $this->dbConn;
    }

    /**
     * closedbConn
     */
    public function closedbConn()
    {
        if ($this->dbConn instanceof Connection && $this->dbConn->isConnected()) {
            $this->dbConn->close();
        }
    }

    
    /**
     * destruct
     */
    public function __destruct()
    {
        $this->closedbConn();
    }

}