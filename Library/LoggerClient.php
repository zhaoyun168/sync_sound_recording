<?php
/**
 * LoggerClient
 */
namespace Library;

use Monolog\Handler\StreamHandler;
use Monolog\Logger;

/**
 * Class LoggerClient
 * @package Library
 */
class LoggerClient
{

    /**
     * @var Object
     */
    private $monolog;
    /**
     * @var Object
     */
    private $config;

    /**
     * LoggerClient constructor.
     * @param $config
     */
    public function __construct($config)
    {
        $this->config=$config;
    }

    /**
     * @param string $name
     * @param string $folder
     * @param int $level
     * @return Logger|Object
     */
    public function getMonolog($name='migrate',$folder='',$level=4){
        switch($level){
            case 0:
                $level=Logger::DEBUG;
                break;
            case 1:
                $level=Logger::INFO;
                break;
            case 2:
                $level=Logger::NOTICE;
                break;
            case 3:
                $level=Logger::WARNING;
                break;
            case 4:
                $level=Logger::ERROR;
                break;
            case 5:
                $level=Logger::CRITICAL;
                break;
            case 6:
                $level=Logger::ALERT;
                break;
            case 7:
                $level=Logger::EMERGENCY;
                break;
        }

        $this->monolog = new Logger($name);
        $this->monolog->pushHandler(
            new StreamHandler($this->config['log_dir'] . '/' . $folder, $level)
        );
        return $this->monolog;
    }

}

