# Auto Init Cluster
This project helps users automatically initialize new clusters to deploy [Hadoop](https://hadoop.apache.org/) 
and [Spark](https://spark.apache.org/), from `tar` to `hdfs` or `spark-submit`.

自动初始化部署Hadoop和Spark集群，从软件的压缩包开始一键部署，获得可以直接使用的命令/接口。

*Now for CentOS 7.*

*现在仅在CentOS 7上进行了测试。*

## Requirements
* fabric==2.5.0
* toml==0.10.0

## Usage
1. Download bin including JDK, Hadoop, Scala and Spark.

2. Modify *config.toml*
    * `xxx_source`: local relative path of JDK, Hadoop, Scala and Spark tar. 
    * `xxx_path`: remote dir of JDK, Hadoop, Scala and Spark.
    * `username` and `password`: cluster user and pwd (no need to sudo)

3. Generate *passphrase.toml*
    * `username` and `password`: user with sudo to create cluster user
    
4. `fab` to auto init
    * use `fab --list` to show all task
    * follow the order in *bootstrap.sh* or choose the task you want

5. Check and test on Hadoop and Spark
    * Web UI
    * `spark-submit` command in *bootstrap.sh*

## Reference
[init_ec2](https://github.com/jixuan1989/init_ec2):
a project to initialize a new cluster on Ubuntu based on Python2.7 and fabric 1.x. 

