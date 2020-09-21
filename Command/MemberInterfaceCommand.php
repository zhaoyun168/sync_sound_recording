<?php
/**
 * 会员接口数据拉取入库脚本
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
 * Class MemberInterfaceCommand
 *
 * @package Command
 */
class MemberInterfaceCommand extends Command
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
     * @var Url List
     */
    private $urlList = [
        'shops' => 'https://ksk001.com/api/v103/public/shops', //店铺列表
        'category' => 'https://ksk001.com/api/v103/public/category', //商品分类
        'goods' => 'https://ksk001.com/api/v103/public/goods', //产品列表
        'weigh' => 'https://ksk001.com/api/v103/public/weigh', //获取收银秤IP对照表
        'member_class' => 'https://ksk001.com/api/v103/public/member_class', //获取会员卡类型列表
        'member_information' => 'https://ksk001.com/api/v103/public/member_information', //获取会员信息表
        'member_record' => 'https://ksk001.com/api/v103/public/member_record', //获取会员卡记录
        'amount_document' => 'https://ksk001.com/api/v103/public/amount_document', //获取金额单据表
        'storage_record' => 'https://ksk001.com/api/v103/public/storage_record', //获取入库记录
        'sale_record' => 'https://ksk001.com/api/v103/public/sale_record', //商品销售记录
        'goods_price' => 'https://ksk001.com/api/v103/public/goods_price', //门店商品价格
    ];
    /**
     * @var Appid
     */
    const APPID = 'cMAly2H2f4tNG34w985W';
    /**
     * @var Key
     */
    const KEY = '8w3DrJWsb3gcyA5ov19cNJNhHIyuVE';
    /**
     * @var Time Out
     */
    const TIMEOUT = 5;
    /**
     * @var 每页几条数据
     */
    const PERPAGE = 100;
    /**
     * @var 最大页码
     */
    const MAXPAGE = 10000;

    /**
     * MemberInterfaceCommand constructor.
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
        $this->setName(sprintf('%s:%s', 'member', 'interface'))
            ->setDescription('Member Interface Data')
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
            'info',
        );
        if (!in_array($t['type'], $typeArray)) {
            $output->writeln(sprintf('type should be %s', implode(' ', $typeArray)));
            exit(0);
        }

        switch ($t['type']) {
            case 'info': // 获取会员基本数据
                $this->memberBaseInfo($output);
                break;
        }
        return null;
    }

    /**
     * @param $output
     * @return bool
     * @throws \Doctrine\DBAL\DBALException
     */
    private function memberBaseInfo($output)
    {
        $output->writeln(sprintf('[%s]', date('Y-m-d H:i:s')));
        $startTime = microtime(true);
        
        //获取店铺列表
        //$this->getShops();
        //获取商品分类
        //$this->getCategory();
        //获取产品列表
        //$this->getGoods();
        //获取收银秤IP对照表
        //$this->getWeigh();
        //获取会员卡类型列表
        //$this->getMemberClass();
        //获取会员信息表
        //$this->getMemberInformation();
        //获取会员卡记录
        //$this->getMemberRecord();
        //获取金额单据表
        //$this->getamountDocument();
        //获取入库记录
        //$this->getStorageRecord();
        //商品销售记录
        //$this->getSaleRecord();
        //门店商品价格
        $this->getGoodsPrice();


        $endTime = microtime(true);
        $output->writeln(sprintf('member interface have finished successfully, time:%s', ($endTime - $startTime)));
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

    /**
     * 验证参数
     * @return array
     */
    private function getToken()
    {
        $token = [];

        $nonceStr = md5(uniqid(microtime(true), true)); //随机字符串
        $timeStamp = time();
        $token['nonce_str'] = $nonceStr;
        $token['token'] = strtoupper(md5($nonceStr . SELF::KEY . $timeStamp));
        $token['time_stamp'] = $timeStamp;
        $token['appid'] = self::APPID;

        return $token;
    }

    /**
     * 获取店铺列表并入库
     * @return null
     */
    private function getShops()
    {
        $this->dbConn = $this->mysqlClient->getdbConn();
        if (is_null($this->dbConn)) {
            $output->writeln('database connection error and stop execute');
            exit(0);
        }

        //获取数据库商铺列表
        $shopsId = $this->getDbShops();

        try{
            $token = $this->getToken();
            $result = $this->sendHttpRequest($this->urlList['shops'], 'GET', SELF::TIMEOUT, $token);

            $this->dbConn->beginTransaction();

            if (is_array($result) && !empty($result)) {
                foreach ($result as $key => $value) {
                    if (isset($value['id']) && !empty($value['id'])) {
                        $shopName = isset($value['shop_name']) && !empty($value['shop_name']) ? $value['shop_name'] : '';

                        //重复验证
                        if (!in_array($value['id'], $shopsId)) {
                            $shopsData = [
                                'id' => (int) $value['id'],
                                'shop_name' => $shopName,
                            ];
                            $this->dbConn->insert('shops', $shopsData);
                        } else {
                            $this->dbConn->update('shops', 
                                [
                                    'shop_name' => $shopName,
                                ],
                                [
                                    'id' => (int) $value['id'],
                                ]
                            );
                            $this->logger->info(sprintf('shops数据更新id[%s]shop_name[%s]', $value['id'], $shopName));
                        }
                    }
                }
            }
            
            $this->dbConn->commit();
        }catch(\Error $e){
            $this->dbConn->rollBack();
            $this->logger->error(sprintf('get shops error:%s', $e->getMessage()));
            exit(0);
        }
    }

    /**
     * 获取数据库店铺列表
     * @return array
     */
    private function getDbShops()
    {
        $shopsRow = $this->dbConn->fetchAll("SELECT id FROM shops");

        return array_column($shopsRow, 'id');
    }

    /**
     * 获取商品分类并入库
     * @return null
     */
    private function getCategory()
    {
        $this->dbConn = $this->mysqlClient->getdbConn();
        if (is_null($this->dbConn)) {
            $output->writeln('database connection error and stop execute');
            exit(0);
        }

        //获取数据库商品分类
        $categoryId = $this->getDbCategory();

        try{
            $token = $this->getToken();
            $result = $this->sendHttpRequest($this->urlList['category'], 'GET', SELF::TIMEOUT, $token);

            $this->dbConn->beginTransaction();

            if (is_array($result) && !empty($result)) {
                foreach ($result as $key => $value) {
                    if (isset($value['id']) && !empty($value['id'])) {
                        $categoryName = isset($value['category_name']) && !empty($value['category_name']) ? $value['category_name'] : '';

                        //重复验证
                        if (!in_array($value['id'], $categoryId)) {
                            $categoryData = [
                                'id' => (int) $value['id'],
                                'category_name' => $categoryName,
                            ];
                            $this->dbConn->insert('category', $categoryData);
                        } else {
                            $this->dbConn->update('category', 
                                [
                                    'category_name' => $categoryName,
                                ],
                                [
                                    'id' => (int) $value['id'],
                                ]
                            );
                            $this->logger->info(sprintf('category数据更新id[%s]category_name[%s]', $value['id'], $categoryName));
                        }
                    }
                }
            }
            
            $this->dbConn->commit();
        }catch(\Error $e){
            $this->dbConn->rollBack();
            $this->logger->error(sprintf('get category error:%s', $e->getMessage()));
            exit(0);
        }
    }

    /**
     * 获取数据库商品分类
     * @return array
     */
    private function getDbCategory()
    {
        $categoryRow = $this->dbConn->fetchAll("SELECT id FROM category");

        return array_column($categoryRow, 'id');
    }

    /**
     * 获取产品列表
     * @return null
     */
    private function getGoods()
    {
        $this->dbConn = $this->mysqlClient->getdbConn();
        if (is_null($this->dbConn)) {
            $output->writeln('database connection error and stop execute');
            exit(0);
        }

        $categoryIDName = $this->getDbCategoryIDName();

        try{
            $maxPage = self::MAXPAGE;

            for ($i=1; $i <= $maxPage; $i++) {
                $token = $this->getToken();
                $param = [
                    'page' => $i,
                    'per_page' => self::PERPAGE
                ];
                $getParam = array_merge($token, $param);
                $result = $this->sendHttpRequest($this->urlList['goods'], 'GET', SELF::TIMEOUT, $getParam);

                if (!empty($result)) {
                    $this->dbConn->beginTransaction();

                    if (is_array($result) && !empty($result)) {
                        foreach ($result as $key => $value) {
                            if (isset($value['id']) && !empty($value['id'])) {
                                $categoryId = isset($value['category']) && !empty($value['category']) && in_array($value['category'], $categoryIDName) ? array_search($value['category'], $categoryIDName) : 0;

                                $insertData = [
                                    'category_id' => $categoryId,
                                    'goods_no' => isset($value['goods_no']) && !empty($value['goods_no']) ? $value['goods_no'] : '',
                                    'goods_name' => isset($value['goods_name']) && !empty($value['goods_name']) ? $value['goods_name'] : '',
                                    'standard' => isset($value['standard']) && !empty($value['standard']) ? $value['standard'] : '',
                                    'unit' => isset($value['unit']) && !empty($value['unit']) ? $value['unit'] : '',
                                    'barcode' => isset($value['barcode']) && !empty($value['barcode']) ? $value['barcode'] : '',
                                    'stor_good_no' => isset($value['stor_good_no']) && !empty($value['stor_good_no']) ? $value['stor_good_no'] : '',
                                ];

                                //重复验证
                                $chenkNum = $this->dbConn->fetchColumn('SELECT id FROM goods WHERE id = :id LIMIT 1', array('id' => (int) $value['id']), 0, array('id' => \PDO::PARAM_INT));
                                $chenkNumFlag = empty($chenkNum) ? false : true;
                                if (!$chenkNumFlag) {
                                    $goodsData = [
                                        'id' => (int) $value['id'],
                                    ];
                                    $goodsData = array_merge($goodsData, $insertData);
                                    $this->dbConn->insert('goods', $goodsData);
                                } else {
                                    $this->dbConn->update('goods', 
                                        $insertData,
                                        [
                                            'id' => (int) $value['id'],
                                        ]
                                    );
                                    $this->logger->info(sprintf('goods数据更新id[%s]info[%s]', $value['id'], json_encode($insertData)));
                                }
                            }
                        }
                    }
                    
                    $this->dbConn->commit();
                } else {
                    break;
                }
            }
        }catch(\Error $e){
            $this->dbConn->rollBack();
            $this->logger->error(sprintf('get goods error:%s', $e->getMessage()));
            exit(0);
        }
    }

    /**
     * 获取数据库商品分类关联列表
     * @return array
     */
    private function getDbCategoryIDName()
    {
        $categoryRow = $this->dbConn->fetchAll("SELECT id,category_name FROM category");

        return array_column($categoryRow, 'category_name', 'id');
    }

    /**
     * 获取收银秤IP对照表
     * @return null
     */
    private function getWeigh()
    {
        $this->dbConn = $this->mysqlClient->getdbConn();
        if (is_null($this->dbConn)) {
            $output->writeln('database connection error and stop execute');
            exit(0);
        }

        $shopIDName = $this->getDbShopsIDName();

        try{
            if (is_array($shopIDName) && !empty($shopIDName)) {
                foreach ($shopIDName as $shopKey => $shopValue) {
                    $token = $this->getToken();
                    $param = [
                        'shop_name' => $shopValue
                    ];
                    $getParam = array_merge($token, $param);
                    $result = $this->sendHttpRequest($this->urlList['weigh'], 'GET', SELF::TIMEOUT, $getParam);

                    $this->dbConn->beginTransaction();

                    if (is_array($result) && !empty($result)) {
                        foreach ($result as $key => $value) {
                            if (isset($value['id']) && !empty($value['id'])) {
                                $shopName = isset($value['shop_name']) && !empty($value['shop_name']) ? $value['shop_name'] : '';
                                $insertData = [
                                    'shop_id' => in_array($shopName, $shopIDName) ? array_search($shopName, $shopIDName) : 0, //店铺ID
                                    'name' => isset($value['name']) && !empty($value['name']) ? $value['name'] : '', //秤名
                                    'ip' => isset($value['ip']) && !empty($value['ip']) ? $value['ip'] : '', //ip地址
                                    'model ' => isset($value['model ']) && !empty($value['model ']) ? $value['model '] : '', //类型
                                    'remark' => isset($value['remark']) && !empty($value['remark']) ? $value['remark'] : '', //备注
                                ];

                                //重复验证
                                $chenkNum = $this->dbConn->fetchColumn('SELECT id FROM weigh WHERE id = :id LIMIT 1', array('id' => (int) $value['id']), 0, array('id' => \PDO::PARAM_INT));
                                $chenkNumFlag = empty($chenkNum) ? false : true;
                                if (!$chenkNumFlag) {
                                    $weighData = [
                                        'id' => (int) $value['id'],
                                    ];
                                    $weighData = array_merge($weighData, $insertData);
                                    $this->dbConn->insert('weigh', $weighData);
                                } else {
                                    $this->dbConn->update('weigh', 
                                        $insertData,
                                        [
                                            'id' => (int) $value['id'],
                                        ]
                                    );
                                    $this->logger->info(sprintf('weigh数据更新id[%s]info[%s]', $value['id'], json_encode($insertData)));
                                }
                            }
                        }
                    }
                    
                    $this->dbConn->commit();
                }
            }
        }catch(\Error $e){
            $this->dbConn->rollBack();
            $this->logger->error(sprintf('get weigh error:%s', $e->getMessage()));
            exit(0);
        }
    }

    /**
     * 获取数据库商铺关联列表
     * @return array
     */
    private function getDbShopsIDName()
    {
        $shopRow = $this->dbConn->fetchAll("SELECT id,shop_name FROM shops");

        return array_column($shopRow, 'shop_name', 'id');
    }

    /**
     * 获取会员卡类型列表
     * @return null
     */
    private function getMemberClass()
    {
        $this->dbConn = $this->mysqlClient->getdbConn();
        if (is_null($this->dbConn)) {
            $output->writeln('database connection error and stop execute');
            exit(0);
        }

        //获取数据库会员卡类型列表
        $memberClassId = $this->getDbMemberClass();

        try{
            $token = $this->getToken();
            $result = $this->sendHttpRequest($this->urlList['member_class'], 'GET', SELF::TIMEOUT, $token);

            $this->dbConn->beginTransaction();

            if (is_array($result) && !empty($result)) {
                foreach ($result as $key => $value) {
                    if (isset($value['id']) && !empty($value['id'])) {

                        $insertData = [
                            'name' => isset($value['name']) && !empty($value['name']) ? $value['name'] : '',
                            'price' => isset($value['price']) && !empty($value['price']) ? $value['price'] : 0,
                            'validity' => isset($value['validity']) && !empty($value['validity']) ? $value['validity'] : '',
                            'integral_ratia' => isset($value['integral_ratia']) && !empty($value['integral_ratia']) ? $value['integral_ratia'] : 0,
                            'discount_ratia' => isset($value['discount_ratia']) && !empty($value['discount_ratia']) ? $value['discount_ratia'] : 0,
                            'amount' => isset($value['amount']) && !empty($value['amount']) ? $value['amount'] : 0,
                            'integral' => isset($value['integral']) && !empty($value['integral']) ? $value['integral'] : 0,
                            'price_class' => isset($value['price_class']) && !empty($value['price_class']) ? $value['price_class'] : '',
                            'recharge_integral_ratia' => isset($value['recharge_integral_ratia']) && !empty($value['recharge_integral_ratia']) ? $value['recharge_integral_ratia'] : 0,
                            'low_amount' => isset($value['low_amount']) && !empty($value['low_amount']) ? $value['low_amount'] : 0,
                        ];

                        //重复验证
                        if (!in_array($value['id'], $memberClassId)) {
                            $memberClassData = [
                                'id' => (int) $value['id'],
                            ];
                            $memberClassData = array_merge($memberClassData, $insertData);
                            $this->dbConn->insert('member_class', $memberClassData);
                        } else {
                            $this->dbConn->update('member_class', 
                                $insertData,
                                [
                                    'id' => (int) $value['id'],
                                ]
                            );
                            $this->logger->info(sprintf('member_class数据更新id[%s]info[%s]', $value['id'], json_encode($insertData)));
                        }
                    }
                }
            }
            
            $this->dbConn->commit();
        }catch(\Error $e){
            $this->dbConn->rollBack();
            $this->logger->error(sprintf('get member class error:%s', $e->getMessage()));
            exit(0);
        }
    }

    /**
     * 获取会员卡类型列表
     * @return array
     */
    private function getDbMemberClass()
    {
        $memberClassRow = $this->dbConn->fetchAll("SELECT id FROM member_class");

        return array_column($memberClassRow, 'id');
    }

    /**
     * 获取会员信息表
     * @return null
     */
    private function getMemberInformation()
    {
        $this->dbConn = $this->mysqlClient->getdbConn();
        if (is_null($this->dbConn)) {
            $output->writeln('database connection error and stop execute');
            exit(0);
        }

        //获取会员信息表
        $memberInformationId = $this->getDbMemberInformation();

        try{
            $token = $this->getToken();
            $result = $this->sendHttpRequest($this->urlList['member_information'], 'GET', SELF::TIMEOUT, $token);

            $this->dbConn->beginTransaction();

            if (is_array($result) && !empty($result)) {
                foreach ($result as $key => $value) {
                    if (isset($value['id']) && !empty($value['id'])) {

                        $insertData = [
                            'c_CardNO' => isset($value['c_CardNO']) && !empty($value['c_CardNO']) ? $value['c_CardNO'] : '',
                            't_CreateTime' => isset($value['t_CreateTime']) && !empty($value['t_CreateTime']) ? date('Y-m-d H:i:s', strtotime($value['t_CreateTime'])) : '',
                            'c_ClassName' => isset($value['c_ClassName']) && !empty($value['c_ClassName']) ? $value['c_ClassName'] : '',
                            'c_Mobile' => isset($value['c_Mobile']) && !empty($value['c_Mobile']) ? $value['c_Mobile'] : '',
                            'c_Name' => isset($value['c_Name']) && !empty($value['c_Name']) ? $value['c_Name'] : '',
                            'c_Password' => isset($value['c_Password']) && !empty($value['c_Password']) ? $value['c_Password'] : '',
                            'C_PriceClass' => isset($value['C_PriceClass']) && !empty($value['C_PriceClass']) ? $value['C_PriceClass'] : '',
                            'n_DiscountValue' => isset($value['n_DiscountValue']) && !empty($value['n_DiscountValue']) ? $value['n_DiscountValue'] : '',
                            'n_IntegralValue' => isset($value['n_IntegralValue']) && !empty($value['n_IntegralValue']) ? $value['n_IntegralValue'] : 0,
                            'n_AmountAvailable' => isset($value['n_AmountAvailable']) && !empty($value['n_AmountAvailable']) ? $value['n_AmountAvailable'] : 0,
                            'n_IntegralAvailable' => isset($value['n_IntegralAvailable']) && !empty($value['n_IntegralAvailable']) ? $value['n_IntegralAvailable'] : 0,
                            't_StopTime' => isset($value['t_StopTime']) && !empty($value['t_StopTime']) ? date('Y-m-d H:i:s', strtotime($value['t_StopTime'])) : 0,
                            'n_IntegralAccumulated' => isset($value['n_IntegralAccumulated']) && !empty($value['n_IntegralAccumulated']) ? $value['n_IntegralAccumulated'] : 0,
                        ];

                        //重复验证
                        if (!in_array($value['id'], $memberInformationId)) {
                            $memberInformationData = [
                                'id' => (int) $value['id'],
                            ];
                            $memberInformationData = array_merge($memberInformationData, $insertData);
                            $this->dbConn->insert('member_information', $memberInformationData);
                        } else {
                            $this->dbConn->update('member_information', 
                                $insertData,
                                [
                                    'id' => (int) $value['id'],
                                ]
                            );
                            $this->logger->info(sprintf('member_information数据更新id[%s]info[%s]', $value['id'], json_encode($insertData)));
                        }
                    }
                }
            }
            
            $this->dbConn->commit();
        }catch(\Error $e){
            $this->dbConn->rollBack();
            $this->logger->error(sprintf('get member information error:%s', $e->getMessage()));
            exit(0);
        }
    }

    /**
     * 获取会员信息表
     * @return array
     */
    private function getDbMemberInformation()
    {
        $memberInformationRow = $this->dbConn->fetchAll("SELECT id FROM member_information");

        return array_column($memberInformationRow, 'id');
    }

    /**
     * 获取会员卡记录
     * @return null
     */
    private function getMemberRecord()
    {
        $this->dbConn = $this->mysqlClient->getdbConn();
        if (is_null($this->dbConn)) {
            $output->writeln('database connection error and stop execute');
            exit(0);
        }

        //获取数据库店铺
        $shopIDName = $this->getDbShopsIDName();

        try{
            $count = $this->dbConn->fetchColumn('SELECT COUNT(*) FROM member_information');
            $perPage = 100;
            $page = ceil($count / $perPage);

            for ($i=1; $i <= $page; $i++) { 
                $memberInformationRow = $this->dbConn->fetchAll("SELECT id,c_CardNO FROM member_information LIMIT ". ($i - 1)*$perPage .",".$perPage);
                foreach ($memberInformationRow as $memberKey => $memberValue) {
                    if (empty($memberValue['c_CardNO'])) {
                        continue;
                    }
                    $token = $this->getToken();
                    $param = [
                        'card_no' => $memberValue['c_CardNO'],
                    ];
                    $getParam = array_merge($token, $param);
                    $result = $this->sendHttpRequest($this->urlList['member_record'], 'GET', SELF::TIMEOUT, $getParam);

                    $this->dbConn->beginTransaction();

                    if (is_array($result) && !empty($result)) {
                        foreach ($result as $key => $value) {
                            if (isset($value['id']) && !empty($value['id'])) {

                                $insertData = [
                                    'card_no' => isset($value['card_no']) && !empty($value['card_no']) ? $value['card_no'] : '',
                                    'shops_id' => isset($value['shop_name']) && !empty($value['shop_name']) ? (in_array($value['shop_name'], $shopIDName) ? array_search($value['shop_name'], $shopIDName) : '') : '',
                                    'member_name' => isset($value['member_name']) && !empty($value['member_name']) ? $value['member_name'] : '',
                                    'user_name' => isset($value['user_name']) && !empty($value['user_name']) ? $value['user_name'] : '',
                                    'change_type' => isset($value['change_type']) && !empty($value['change_type']) ? $value['change_type'] : '',
                                    'amount_inc' => isset($value['amount_inc']) && !empty($value['amount_inc']) ? $value['amount_inc'] : 0,
                                    'amount_dec' => isset($value['amount_dec']) && !empty($value['amount_dec']) ? $value['amount_dec'] : 0,
                                    'amount_available' => isset($value['amount_available']) && !empty($value['amount_available']) ? $value['amount_available'] : 0,
                                    'integral_inc' => isset($value['integral_inc']) && !empty($value['integral_inc']) ? $value['integral_inc'] : 0,
                                    'integral_dec' => isset($value['integral_dec']) && !empty($value['integral_dec']) ? $value['integral_dec'] : 0,
                                    'integral_available' => isset($value['integral_available']) && !empty($value['integral_available']) ? $value['integral_available'] : 0,
                                    'time' => isset($value['time']) && !empty($value['time']) ? date('Y-m-d H:i:s', strtotime($value['time'])) : '',
                                ];

                                //重复验证
                                $chenkNum = $this->dbConn->fetchColumn('SELECT id FROM member_record WHERE id = :id LIMIT 1', array('id' => (int) $value['id']), 0, array('id' => \PDO::PARAM_INT));
                                $chenkNumFlag = empty($chenkNum) ? false : true;
                                if (!$chenkNumFlag) {
                                    $memberRecordData = [
                                        'id' => (int) $value['id'],
                                    ];
                                    $memberRecordData = array_merge($memberRecordData, $insertData);
                                    $this->dbConn->insert('member_record', $memberRecordData);
                                } else {
                                    $this->dbConn->update('member_record', 
                                        $insertData,
                                        [
                                            'id' => (int) $value['id'],
                                        ]
                                    );
                                    $this->logger->info(sprintf('member_record数据更新id[%s]info[%s]', $value['id'], json_encode($insertData)));
                                }
                            }
                        }
                    }
                    
                    $this->dbConn->commit();
                }
            }
        }catch(\Error $e){
            $this->dbConn->rollBack();
            $this->logger->error(sprintf('get member record error:%s', $e->getMessage()));
            exit(0);
        }
    }

    /**
     * 获取金额单据表
     * @return null
     */
    private function getamountDocument()
    {
        $this->dbConn = $this->mysqlClient->getdbConn();
        if (is_null($this->dbConn)) {
            $output->writeln('database connection error and stop execute');
            exit(0);
        }

        //获取数据库店铺
        $shopIDName = $this->getDbShopsIDName();

        try{
            $token = $this->getToken();
            $param = [
                'bill_no' => '',
            ];
            $getParam = array_merge($token, $param);
            $result = $this->sendHttpRequest($this->urlList['amount_document'], 'GET', SELF::TIMEOUT, $getParam);

            $this->dbConn->beginTransaction();

            if (is_array($result) && !empty($result)) {
                foreach ($result as $key => $value) {
                    if (isset($value['id']) && !empty($value['id'])) {

                        $insertData = [
                            'c_BillStatus' => isset($value['c_BillStatus']) && !empty($value['c_BillStatus']) ? $value['c_BillStatus'] : '', //订单状态
                            'shops_id' => isset($value['c_ShopName']) && !empty($value['c_ShopName']) ? (in_array($value['c_ShopName'], $shopIDName) ? array_search($value['c_ShopName'], $shopIDName) : '') : '', //店铺ID
                            'c_BillNO' => isset($value['c_BillNO']) && !empty($value['c_BillNO']) ? $value['c_BillNO'] : '', //单据号
                            'c_BillType' => isset($value['c_BillType']) && !empty($value['c_BillType']) ? $value['c_BillType'] : '', //单据类型
                            'n_PayCash' => isset($value['n_PayCash']) && !empty($value['n_PayCash']) ? $value['n_PayCash'] : 0, //付现金
                            'n_PayCard' => isset($value['n_PayCard']) && !empty($value['n_PayCard']) ? $value['n_PayCard'] : 0, //扣储值卡
                            'n_PayBank' => isset($value['n_PayBank']) && !empty($value['n_PayBank']) ? $value['n_PayBank'] : 0, //银行卡支付
                            'n_PayIntegral' => isset($value['n_PayIntegral']) && !empty($value['n_PayIntegral']) ? $value['n_PayIntegral'] : 0, //积分抵现
                            'n_PayTicket' => isset($value['n_PayTicket']) && !empty($value['n_PayTicket']) ? $value['n_PayTicket'] : 0, //优惠券
                            'n_PayThird' => isset($value['n_PayThird']) && !empty($value['n_PayThird']) ? $value['n_PayThird'] : 0, //第三方支付
                            'n_PayOther' => isset($value['n_PayOther']) && !empty($value['n_PayOther']) ? $value['n_PayOther'] : 0, //其它支付
                            'n_PayShould' => isset($value['n_PayShould']) && !empty($value['n_PayShould']) ? $value['n_PayShould'] : 0, //应付总额
                            'n_PayActual' => isset($value['n_PayActual']) && !empty($value['n_PayActual']) ? $value['n_PayActual'] : 0, //实付总额
                            'n_GetIntegral' => isset($value['n_GetIntegral']) && !empty($value['n_GetIntegral']) ? $value['n_GetIntegral'] : 0, //获得积分
                            't_Time' => isset($value['t_Time']) && !empty($value['t_Time']) ? date('Y-m-d H:i:s', strtotime($value['t_Time'])) : '', //订单时间
                            'c_Remark' => isset($value['c_Remark']) && !empty($value['c_Remark']) ? $value['c_Remark'] : '', //备注
                            'c_UserName' => isset($value['c_UserName']) && !empty($value['c_UserName']) ? $value['c_UserName'] : '', //操作人用户名
                        ];

                        //重复验证
                        $chenkNum = $this->dbConn->fetchColumn('SELECT id FROM amount_document WHERE id = :id LIMIT 1', array('id' => (int) $value['id']), 0, array('id' => \PDO::PARAM_INT));
                        $chenkNumFlag = empty($chenkNum) ? false : true;
                        if (!$chenkNumFlag) {
                            $amountDocumentData = [
                                'id' => (int) $value['id'],
                            ];
                            $amountDocumentData = array_merge($amountDocumentData, $insertData);
                            $this->dbConn->insert('amount_document', $amountDocumentData);
                        } else {
                            $this->dbConn->update('amount_document', 
                                $insertData,
                                [
                                    'id' => (int) $value['id'],
                                ]
                            );
                            $this->logger->info(sprintf('amount_document数据更新id[%s]info[%s]', $value['id'], json_encode($insertData)));
                        }
                    }
                }
            }
            
            $this->dbConn->commit();
        }catch(\Error $e){
            $this->dbConn->rollBack();
            $this->logger->error(sprintf('get amount document error:%s', $e->getMessage()));
            exit(0);
        }
    }

    /**
     * 获取入库记录
     * @return null
     */
    private function getStorageRecord()
    {
        $this->dbConn = $this->mysqlClient->getdbConn();
        if (is_null($this->dbConn)) {
            $output->writeln('database connection error and stop execute');
            exit(0);
        }

        try{
            $token = $this->getToken();
            $result = $this->sendHttpRequest($this->urlList['storage_record'], 'GET', SELF::TIMEOUT, $token);

            $this->dbConn->beginTransaction();

            if (is_array($result) && !empty($result)) {
                foreach ($result as $key => $value) {
                    if (isset($value['id']) && !empty($value['id'])) {

                        $insertData = [
                            'c_BillNO' => isset($value['c_BillNO']) && !empty($value['c_BillNO']) ? $value['c_BillNO'] : '', //入库主单据号
                            'c_GoodsNO' => isset($value['c_GoodsNO']) && !empty($value['c_GoodsNO']) ? $value['c_GoodsNO'] : '', //商品编号
                            'c_GoodsName' => isset($value['c_GoodsName']) && !empty($value['c_GoodsName']) ? $value['c_GoodsName'] : '', //商品名称
                            'c_ClassName' => isset($value['c_ClassName']) && !empty($value['c_ClassName']) ? $value['c_ClassName'] : '', //商品类别名称
                            'c_Unit' => isset($value['c_Unit']) && !empty($value['c_Unit']) ? $value['c_Unit'] : '', //单位名称
                            'n_Price' => isset($value['n_Price']) && !empty($value['n_Price']) ? $value['n_Price'] : 0, //进价
                            'n_Number' => isset($value['n_Number']) && !empty($value['n_Number']) ? $value['n_Number'] : '', //
                            'n_Amount' => isset($value['n_Amount']) && !empty($value['n_Amount']) ? $value['n_Amount'] : '', //
                            'c_AddUser' => isset($value['c_AddUser']) && !empty($value['c_AddUser']) ? $value['c_AddUser'] : '', //
                            't_UpdateTime' => isset($value['t_UpdateTime']) && !empty($value['t_UpdateTime']) ? $value['t_UpdateTime'] : '', //更新日期
                            't_SettlementUpdateTime' => isset($value['t_SettlementUpdateTime']) && !empty($value['t_SettlementUpdateTime']) ? $value['t_SettlementUpdateTime'] : '', //结账更新日期
                        ];

                        //重复验证
                        $chenkNum = $this->dbConn->fetchColumn('SELECT id FROM storage_record WHERE id = :id LIMIT 1', array('id' => (int) $value['id']), 0, array('id' => \PDO::PARAM_INT));
                        $chenkNumFlag = empty($chenkNum) ? false : true;
                        if (!$chenkNumFlag) {
                            $storageRecordData = [
                                'id' => (int) $value['id'],
                            ];
                            $storageRecordData = array_merge($storageRecordData, $insertData);
                            $this->dbConn->insert('storage_record', $storageRecordData);
                        } else {
                            $this->dbConn->update('storage_record', 
                                $insertData,
                                [
                                    'id' => (int) $value['id'],
                                ]
                            );
                            $this->logger->info(sprintf('storage_record数据更新id[%s]info[%s]', $value['id'], json_encode($insertData)));
                        }
                    }
                }
            }
            
            $this->dbConn->commit();
        }catch(\Error $e){
            $this->dbConn->rollBack();
            $this->logger->error(sprintf('get storage record error:%s', $e->getMessage()));
            exit(0);
        }
    }

    /**
     * 商品销售记录
     * @return null
     */
    private function getSaleRecord()
    {
        $this->dbConn = $this->mysqlClient->getdbConn();
        if (is_null($this->dbConn)) {
            $output->writeln('database connection error and stop execute');
            exit(0);
        }

        try{
            $maxPage = self::MAXPAGE;

            for ($i=1; $i < $maxPage; $i++) {
                $token = $this->getToken();
                $param = [
                    'page' => $i,
                    'per_page' => self::PERPAGE
                ];
                $getParam = array_merge($token, $param);
                $result = $this->sendHttpRequest($this->urlList['sale_record'], 'GET', SELF::TIMEOUT, $getParam);

                if (!empty($result)) {
                    $this->dbConn->beginTransaction();

                    if (is_array($result) && !empty($result)) {
                        foreach ($result as $key => $value) {
                            if (isset($value['id']) && !empty($value['id'])) {

                                $insertData = [
                                    'c_BillStatus' => isset($value['c_BillStatus']) && !empty($value['c_BillStatus']) ? $value['c_BillStatus'] : '', //订单状态
                                    'c_ShopName' => isset($value['c_ShopName']) && !empty($value['c_ShopName']) ? $value['c_ShopName'] : '', //店铺名称
                                    'c_BillNO' => isset($value['c_BillNO']) && !empty($value['c_BillNO']) ? $value['c_BillNO'] : '', //单据号
                                    'c_GoodsName' => isset($value['c_GoodsName']) && !empty($value['c_GoodsName']) ? $value['c_GoodsName'] : '', //商品名称
                                    'n_PriceRetail' => isset($value['n_PriceRetail']) && !empty($value['n_PriceRetail']) ? $value['n_PriceRetail'] : 0, //零售价
                                    'n_Number' => isset($value['n_Number']) && !empty($value['n_Number']) ? $value['n_Number'] : 0, //数量
                                    'c_CardNO' => isset($value['c_CardNO']) && !empty($value['c_CardNO']) ? $value['c_CardNO'] : '', //会员卡号
                                    'c_Name' => isset($value['c_Name']) && !empty($value['c_Name']) ? $value['c_Name'] : '', //会员姓名
                                    'n_PayActual' => isset($value['n_PayActual']) && !empty($value['n_PayActual']) ? $value['n_PayActual'] : 0, //实付总额
                                    'n_GetIntegral' => isset($value['n_GetIntegral']) && !empty($value['n_GetIntegral']) ? $value['n_GetIntegral'] : 0, //获得积分
                                    't_Time' => isset($value['t_Time']) && !empty($value['t_Time']) ? date('Y-m-d H:i:s', strtotime($value['t_Time'])) : '', //订单时间
                                ];

                                //重复验证
                                $chenkNum = $this->dbConn->fetchColumn('SELECT id FROM sale_record WHERE id = :id LIMIT 1', array('id' => (int) $value['id']), 0, array('id' => \PDO::PARAM_INT));
                                $chenkNumFlag = empty($chenkNum) ? false : true;
                                if (!$chenkNumFlag) {
                                    $saleRecordData = [
                                        'id' => (int) $value['id'],
                                    ];
                                    $saleRecordData = array_merge($saleRecordData, $insertData);
                                    $this->dbConn->insert('sale_record', $saleRecordData);
                                } else {
                                    $this->dbConn->update('sale_record', 
                                        $insertData,
                                        [
                                            'id' => (int) $value['id'],
                                        ]
                                    );
                                    $this->logger->info(sprintf('sale_record数据更新id[%s]info[%s]', $value['id'], json_encode($insertData)));
                                }
                            }
                        }
                    }
                    
                    $this->dbConn->commit();
                } else {
                    break;
                }
            }
        }catch(\Error $e){
            $this->dbConn->rollBack();
            $this->logger->error(sprintf('get sale record error:%s', $e->getMessage()));
            exit(0);
        }
    }

    /**
     * 获取门店商品价格
     * @return null
     */
    private function getGoodsPrice()
    {
        $this->dbConn = $this->mysqlClient->getdbConn();
        if (is_null($this->dbConn)) {
            $output->writeln('database connection error and stop execute');
            exit(0);
        }

        //获取数据库商品分类
        $categoryList = $this->getDbCategoryList();
        //获取数据库店铺
        $shopIDName = $this->getDbShopsIDName();

        try{
            if (!empty($categoryList) && is_array($categoryList)) {
                foreach ($categoryList as $listKey => $listKalue) {
                    $token = $this->getToken();
                    $param = [
                        'shop_name' => isset($shopIDName[$listKalue['shops_id']]) && !empty($shopIDName[$listKalue['shops_id']]) ? $shopIDName[$listKalue['shops_id']] : '', //门店名称
                        'goods_classify' => $listKalue['category_name'], //商品分类
                    ];
                    $getParam = array_merge($token, $param);
                    $result = $this->sendHttpRequest($this->urlList['goods_price'], 'GET', SELF::TIMEOUT, $getParam);

                    $this->dbConn->beginTransaction();

                    if (is_array($result) && !empty($result)) {
                        foreach ($result as $key => $value) {
                            if (isset($value['id']) && !empty($value['id'])) {
                                $insertData = [
                                    'c_ShopName' => isset($value['c_ShopName']) && !empty($value['c_ShopName']) ? $value['c_ShopName'] : '', //门店名称
                                    'c_GoodsNO' => isset($value['c_GoodsNO']) && !empty($value['c_GoodsNO']) ? $value['c_GoodsNO'] : '', //商品编号
                                    'c_GoodsName' => isset($value['c_GoodsName']) && !empty($value['c_GoodsName']) ? $value['c_GoodsName'] : '', //商品名称
                                    'n_Number' => isset($value['n_Number']) && !empty($value['n_Number']) ? $value['n_Number'] : 0, //商品数量
                                    'n_Price' => isset($value['n_Price']) && !empty($value['n_Price']) ? $value['n_Price'] : 0, //商品成本单价
                                    'n_PriceRetail' => isset($value['n_PriceRetail']) && !empty($value['n_PriceRetail']) ? $value['n_PriceRetail'] : 0, //商品销售单价
                                    'n_LastPrice' => isset($value['n_LastPrice']) && !empty($value['n_LastPrice']) ? $value['n_LastPrice'] : 0, //商品最后入库单价
                                ];

                                //重复验证
                                $chenkNum = $this->dbConn->fetchColumn('SELECT id FROM goods_price WHERE id = :id LIMIT 1', array('id' => (int) $value['id']), 0, array('id' => \PDO::PARAM_INT));
                                $chenkNumFlag = empty($chenkNum) ? false : true;
                                if (!$chenkNumFlag) {
                                    $goodsPriceData = [
                                        'id' => (int) $value['id'],
                                    ];
                                    $goodsPriceData = array_merge($goodsPriceData, $insertData);
                                    $this->dbConn->insert('goods_price', $goodsPriceData);
                                } else {
                                    $this->dbConn->update('goods_price', 
                                        $insertData,
                                        [
                                            'id' => (int) $value['id'],
                                        ]
                                    );
                                    $this->logger->info(sprintf('goods_price数据更新id[%s]info[%s]', $value['id'], json_encode($insertData)));
                                }
                            }
                        }
                    }
                    
                    $this->dbConn->commit();
                }
            }
        }catch(\Error $e){
            $this->dbConn->rollBack();
            $this->logger->error(sprintf('get goods price error:%s', $e->getMessage()));
            exit(0);
        }
    }

    /**
     * 获取数据库商品分类列表
     * @return array
     */
    private function getDbCategoryList()
    {
        $categoryRow = $this->dbConn->fetchAll("SELECT id,category_name,shops_id FROM category");

        return $categoryRow;
    }
}
