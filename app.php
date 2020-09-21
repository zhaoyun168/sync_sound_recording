<?php
/**
 * app.php
 */

set_time_limit(0);
ini_set('memory_limit', '2048M');
ini_set('date.timezone','Asia/Shanghai');
require __DIR__.'/vendor/autoload.php';

use Symfony\Component\Console\Application;
use Symfony\Component\Yaml\Yaml;

$arr = Yaml::parse(file_get_contents(__DIR__.DIRECTORY_SEPARATOR.'config.yml'));
$config = $arr['parameters'];

$application = new Application();
$application->add(new \Command\MemberInterfaceCommand($config));
$application->run();