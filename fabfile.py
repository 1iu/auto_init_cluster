#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
 @desc:
 @author: @1iu
 @contact: atliuning@gmail.com
 @date: 2019/11/1
"""
from utils.config import ClusterConfig, PassPhrase
from utils.helper import *
from fabric import task
from fabric import Connection, Config
from invoke import Responder

from tqdm import tqdm

import os

user_config = PassPhrase('./passphrase.toml')
config = ClusterConfig('./config.toml')

ssh_config_dict = {'sudo': {'password': user_config.password},
                   'user': user_config.username,
                   'connect_kwargs': {'password': user_config.password}}
ssh_config = Config(overrides=ssh_config_dict)

init_config_dict = {'user': config.server.username,
                    'connect_kwargs': {'password': config.server.password}}
init_config = Config(overrides=init_config_dict)

sudo_conn = GroupHelper(*config.server.hosts, config=ssh_config)
conn = GroupHelper(*config.server.hosts, config=init_config)


@task
def create_user(c):
    passwd = Responder(pattern='[\s\S]*(新的 密码：|[N|n]ew password:)', response='{}\n'.format(config.server.password))

    sudo_conn.sudo("adduser {}".format(config.server.username), pty=True, warn=True)
    sudo_conn.sudo("passwd {}".format(config.server.username), pty=True, watchers=[passwd])


@task()
def remove_user(c):
    sudo_conn.sudo("userdel {}".format(config.server.username), pty=True, warn=True)
    sudo_conn.sudo("rm -rf /home/{}".format(config.server.username), pty=True, warn=True)


@task
def clean_host(c):
    for host, hostname in tqdm(zip(config.server.hosts, config.server.hostnames), total=len(config.server.hosts),
                               desc='clean-host'):
        sudo_conn.sudo("sed -i '/{}\s*{}/d' /etc/hosts".format(host, hostname), pty=True, hide='stderr')


@task
def add_host(c):
    host_cmd = '''sh -c "echo '{}' >> /etc/hosts"'''.format(
        generate_hosts(config.server.hosts, config.server.hostnames))
    sudo_conn.sudo(host_cmd, pty=True)


@task
def clean_key(c):
    conn.run("rm ~/.ssh/authorized_keys")


@task
def add_key(c):
    gen_key = Responder(pattern='Enter.*', response='\n')
    overwrite = Responder(pattern='Overwrite.*', response='y\n')

    conn.run('ssh-keygen -t rsa', pty=True, watchers=[gen_key, overwrite], warn=True)

    os.makedirs('files', exist_ok=True)
    auth_key_path = os.path.join('files', 'authorized_keys')
    print('get id_rsa.pub')
    with open(auth_key_path, 'wb') as key_file:
        conn.get(os.path.join('/home', config.server.username, '.ssh/id_rsa.pub'), key_file)
    print('write authorized_keys')
    with open(auth_key_path, 'r') as key_file:
        keys = key_file.read()
        conn.run("echo '{}' >> ~/.ssh/authorized_keys && chmod 600 ~/.ssh/authorized_keys".format(keys), pty=True)


@task
def clean_bashrc(c):
    conn.run("sed -i '/export JAVA_HOME=.*/d' ~/.bashrc")
    conn.run("sed -i '/export JRE_HOME=.*/d' ~/.bashrc")
    conn.run("sed -i '/export CLASSPATH=.*/d' ~/.bashrc")
    conn.run("sed -i '/export SCALA_HOME=.*/d' ~/.bashrc")
    # add your pattern here....


@task
def install_jdk(c):
    jdk_source = os.path.join('/home', config.server.username, 'jdk.tar.gz')
    jdk_path = os.path.join('/home', config.server.username, config.server.jdk_path)
    print('put jdk...')
    conn.put(config.server.jdk_source, jdk_source)
    print('install jdk...')
    conn.run("mkdir {}".format(jdk_path))
    conn.run("tar -zxf {}".format(jdk_source), pty=True)
    print('configure bashrc...')
    conn.run("echo 'export JAVA_HOME={}'>>~/.bashrc".format(jdk_path), pty=True)
    conn.run("echo 'export JRE_HOME=$JAVA_HOME/jre'>>~/.bashrc", pty=True)
    conn.run("echo 'export PATH=$JAVA_HOME/bin:$JAVA_HOME/jre/bin:$PATH'>>~/.bashrc", pty=True)
    conn.run("echo 'export CLASSPATH=$CLASSPATH:$JAVA_HOME/lib:$JAVA_HOME/jre/lib'>>~/.bashrc", pty=True)
    # clean
    print('clean...')
    conn.run("rm {}".format(jdk_source), pty=True)


@task
def install_scala(c):
    scala_source = os.path.join('/home', config.server.username, 'scala.tar.gz')
    scala_path = os.path.join('/home', config.server.username, config.server.scala_path)
    print('put scala...')
    conn.put(config.server.scala_source, scala_source)
    print('install scala...')
    conn.run("mkdir {}".format(scala_path))
    conn.run("tar -zxf {}".format(scala_source), pty=True)
    print('configure bashrc...')
    conn.run("echo 'export SCALA_HOME={}'>>~/.bashrc".format(scala_path), pty=True)
    conn.run("echo 'export PATH=$SCALA_HOME/bin:$PATH'>>~/.bashrc", pty=True)
    # clean
    print('clean...')
    conn.run("rm {}".format(scala_source), pty=True)


@task
def set_ntp(c):
    ntp_server = config.server.ntp_server
    ntp_conn = Connection(ntp_server, config=ssh_config)
    ntp_conn.sudo('service ntpd restart', pty=True)

    sudo_conn.sudo('ntpdate {}'.format(ntp_server), pty=True, warn=True)
    sudo_conn.sudo('''sh -c "echo '#!/bin/bash' > /etc/cron.daily/myntp"''', pty=True)
    sudo_conn.sudo('''sh -c "echo 'ntpdate {}' >> /etc/cron.daily/myntp"'''.format(ntp_server), pty=True)
    sudo_conn.sudo('chmod 755 /etc/cron.daily/myntp', pty=True)

    ntp_conn.sudo('rm -f /etc/cron.daily/myntp', pty=True, warn=True)


@task
def install_hadoop(c):
    hadoop_source = os.path.join('/home', config.server.username, 'hadoop.tar.gz')
    hadoop_path = os.path.join('/home', config.server.username, config.server.hadoop_path)
    hadoop_config_path = os.path.join(hadoop_path, 'etc/hadoop')
    local_config_path = './files/hadoop'

    # print('put hadoop...')
    # conn.put(config.server.hadoop_source, hadoop_source)
    # print('install hadoop...')
    # conn.run("tar -zxf {}".format(hadoop_source), pty=True)
    # print('configure bashrc...')
    # conn.run("echo 'export HADOOP_HOME={}'>>~/.bashrc".format(hadoop_path), pty=True)
    # conn.run("echo 'export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH'>>~/.bashrc", pty=True)
    # # clean
    # print('clean...')
    # conn.run("rm {}".format(hadoop_source), pty=True)
    # print('configure hadoop-env.sh')
    # jdk_path = os.path.join('/home', config.server.username, config.server.jdk_path)
    # hadoop_env_path = os.path.join(hadoop_config_path, 'hadoop-env.sh')
    # jdk_cmd = "sed -i 's/.*export JAVA_HOME=.*/export JAVA_HOME={}/g' {}".format(jdk_path.replace("/", "\\/"),
    #                                                                              hadoop_env_path)
    # conn.run(jdk_cmd)

    print('configure hadoop/workers & master')
    worker_info = '\n'.join(config.hadoop_hostname)
    conn.run("echo '{}' > {}/workers".format(worker_info, hadoop_config_path))
    conn.run("echo '{}' > {}/master".format(config.hadoop_master, hadoop_config_path))

    print('configure hadoop/core-site.xml')
    conn.put(os.path.join(local_config_path, 'core-site.xml'), hadoop_config_path)
    core_path = os.path.join(hadoop_config_path, 'core-site.xml')
    tmp_path = os.path.join(hadoop_path, config.hadoop_tmp_folder)
    tmp_dir_cmd = "sed -i 's/TEMP_DIR/{}/g' {}".format(tmp_path.replace("/", "\\/"), core_path)
    conn.run(tmp_dir_cmd)
    modeify_ip = "sed -i 's/MASTERIP/{}/g' {}".format(config.hadoop_master, core_path)
    conn.run(modeify_ip)

    print('configure hadoop/hdfs-site.xml')
    conn.put(os.path.join(local_config_path, 'hdfs-site.xml'), hadoop_config_path)
    hdfs_path = os.path.join(hadoop_config_path, 'hdfs-site.xml')
    data_dir = config.hadoop_data_folder
    data_dir_cmd = "sed -i 's/DATA_DIR/{}/g' {}".format(data_dir.replace("/", "\\/"), hdfs_path)
    conn.run(data_dir_cmd)

    print('configure hadoop/yarn-site.xml')
    yarn_path = os.path.join(hadoop_config_path, 'yarn-site.xml')
    conn.put(os.path.join(local_config_path, 'yarn-site.xml'), hadoop_config_path)
    modeify_ip = "sed -i 's/MASTERIP/{}/g' {}".format(config.hadoop_master, yarn_path)
    conn.run(modeify_ip)

    print('configure hadoop/mapred-site.xml')
    mapred_path = os.path.join(hadoop_config_path, 'mapred-site.xml')
    conn.put(os.path.join(local_config_path, 'mapred-site.xml'), hadoop_config_path)
    modeify_ip = "sed -i 's/MASTERIP/{}/g' {}".format(config.hadoop_master, mapred_path)
    conn.run(modeify_ip)


master = Connection(config.hadoop_master, config=init_config)

@task
def format_hadoop(c):
    master.run('hdfs namenode -format')

@task
def chown(c):
    sudo_conn.sudo("sed -i '/.*\/data/d' /etc/rc.local", pty=True, hide='stderr')
    sudo_conn.sudo('''sh -c "echo 'sudo chown -R test:test /data' >> /etc/rc.local"''', pty=True)
    sudo_conn.sudo("sudo chown -R test:test /data", pty=True, warn=True)

@task
def start_hadoop(c):
    master.run('start-dfs.sh')
    master.run('start-yarn.sh')


@task
def stop_hadoop(c):
    master.run('stop-dfs.sh')
    master.run('stop-yarn.sh')


@task
def install_spark(c):
    pass


@task
def whoami(c):
    conn.run('whoami')
    conn.sudo('whoami', hide='stderr')
