# kafka
##  第一次提交
springboot整合定时任务 mybatis
### 思路

#### 定时向数据库插入数据并读取消息数据
	配置pom文件，加入相应的依赖
	配置yml文件
	编写代码
	kafka消息为json或者字符串
	注意：>别名要和实体类一致，任务类别忘记添加@Component注解，启动类别忘记添加@EnableScheduling注解
