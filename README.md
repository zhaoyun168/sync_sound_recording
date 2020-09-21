# 同步员工、部门、录音脚本

### 安装
1. composer install
2. cp config.yml.dist config.yml

### 使用

1. php app.php sync:data user   			# 同步员工信息
2. php app.php sync:data dept   			# 同步部门信息
3. php app.php sync:data sound_recording # 同步录音信息