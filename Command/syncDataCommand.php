<?php
/**
 * 同步员工、部门、录音脚本
 */
namespace Command;

use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Output\OutputInterface;
use Library\MysqlClient;
use Library\LoggerClient;
use GuzzleHttp\Client;

/**
 * Class syncDataCommand
 *
 * @package Command
 */
class syncDataCommand extends Command
{
    /**
     * @var null|string
     */
    protected $config;
    /**
     * @var MysqlClient
     */
    private $mysqlClient;
    /** @var  \Doctrine\DBAL\Connection */
    private $dbConn;
    /**
     * @var \Monolog\Logger|Object
     */
    private $logger;
    /**
     * @var Time Out
     */
    const TIMEOUT = 5;

    /**
     * syncDataCommand constructor.
     * @param null|string $config
     */
    public function __construct($config)
    {
        $this->config = $config;
        $LoggerClient = new LoggerClient($this->config);
        $this->logger = $LoggerClient->getMonolog('member_interface', date('Y-m-d').'.log', 0);
        $this->mysqlClient = new MysqlClient($this->config);
        parent::__construct();
    }

    /**
     * configure
     */
    protected function configure()
    {
        $this->setName(sprintf('%s:%s', 'sync', 'data'))
            ->setDescription('Sync Data')
            ->addArgument('type', InputArgument::REQUIRED, 'type is required and contains info');
    }

    /**
     * @param InputInterface $input
     * @param OutputInterface $output
     * @return null
     */
    protected function execute(InputInterface $input, OutputInterface $output)
    {
        $t = $input->getArguments();
        
        $typeArray = array(
            'user',
            'dept',
            'sound_recording',
        );
        if (!in_array($t['type'], $typeArray)) {
            $output->writeln(sprintf('type should be %s', implode(' ', $typeArray)));
            exit(0);
        }

        switch ($t['type']) {
            case 'user': // 同步员工信息
                $this->syncUser($output);
                break;
            case 'dept': // 同步部门信息
                $this->syncDept($output);
                break;
            case 'sound_recording': // 同步录音信息
                $this->syncSoundRecording($output);
                break;
        }
        return null;
    }

    /**
     * 同步员工信息
     * @param $output
     * @return bool
     * @throws \Doctrine\DBAL\DBALException
     */
    private function syncUser($output)
    {
        $output->writeln(sprintf('[%s]', date('Y-m-d H:i:s')));
        $startTime = microtime(true);

        //开始同步员工信息
        
        $endTime = microtime(true);
        $output->writeln(sprintf('sync user have finished successfully, time:%s', ($endTime - $startTime)));
        return true;
    }

    /**
     * 同步部门信息
     * @param $output
     * @return bool
     * @throws \Doctrine\DBAL\DBALException
     */
    private function syncDept($output)
    {
        $output->writeln(sprintf('[%s]', date('Y-m-d H:i:s')));
        $startTime = microtime(true);

        //开始同步部门信息
        
        $endTime = microtime(true);
        $output->writeln(sprintf('sync user have finished successfully, time:%s', ($endTime - $startTime)));
        return true;
    }

    /**
     * 同步录音信息
     * @param $output
     * @return bool
     * @throws \Doctrine\DBAL\DBALException
     */
    private function syncSoundRecording($output)
    {
        $output->writeln(sprintf('[%s]', date('Y-m-d H:i:s')));
        $startTime = microtime(true);

        //开始同步录音信息
        
        $endTime = microtime(true);
        $output->writeln(sprintf('sync user have finished successfully, time:%s', ($endTime - $startTime)));
        return true;
    }

    /**
     * @param array $url 请求地址
     * @param array $method 请求方式
     * @param array $timeout 超时时间
     * @param array $param 请求参数
     * @return array
     */
    private function sendHttpRequest($url, $method, $timeout, $param){

        if(empty($url)){
            $this->logger->info(sprintf('请求地址为空'));
            return false;
        }else{
            if(!filter_var($url, FILTER_VALIDATE_URL)){
                $this->logger->info(sprintf('请求地址不合法 url:%s', $url));
                return false;
            }
        }

        if(empty($timeout)){
            $this->logger->info(sprintf('超时时间为空'));
            return false;
        }else{
            if(!is_numeric($timeout)){
                $this->logger->info(sprintf('超时时间必须为数字'));
                return false;
            }
        }

        if(empty($method)){
            $this->logger->info(sprintf('请求方式不能为空'));
            return false;
        }else{
            $method = strtoupper(trim($method));
            if(!in_array($method, array('GET','POST'))){
                $this->logger->info(sprintf('发送请求方法为GET或POST'));
                return false;
            }
        }
        $this->logger->info(sprintf('开始发送http请求,url[%s]method[%s]timeout[%s]param[%s]', $url, $method, $timeout, json_encode($param)));
        try {
            $client = new Client(['timeout' => $timeout]);
            switch($method){
                case 'POST':
                    $response=$client->request($method, $url, array(
                        'body' => json_encode($param)
                    ));
                    break;
                case 'GET':
                    $response=$client->request($method, $url, array(
                        'query'=>$param
                    ));
                    break;
            }
            $statusCode = $response->getStatusCode();
            if (200 !== $statusCode) {
                $this->logger->info(sprintf('Guzzle发送http请求失败[%s], 返回状态码为[%s]', json_encode($param), $statusCode));
                return false;
            }
            $body = $response->getBody();
            $contents=$body->getContents();
            $ret = json_decode($contents, true);
            if (JSON_ERROR_NONE !== json_last_error()) {
                $this->logger->info(sprintf('发送http请求参数为[%s], 返回结果[%s]非JSON格式', json_encode($param), $contents));
                return false;
            }
            $this->logger->info(sprintf('发送http请求参数为[%s], 返回结果为[%s]', json_encode($param), $contents));
            if(isset($ret['code']) && $ret['code'] == 200 ){
                if (isset($ret['data'])) {
                    return $ret['data'];
                }
            }

            return [];
        } catch (\Exception $e) {
            $this->logger->error(sprintf('发送http请求异常[%s], 异常信息为[%s]', json_encode($param), $e->getMessage()));
            return false;
        }
    }
}
