#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
 @desc:
 @author: @1iu
 @contact: atliuning@gmail.com
 @date: 2019/11/1
"""
import getpass
from utils.config import ClusterConfig, PassPhrase

from fabric import task
from fabric import Connection, Config
from invoke import Responder


user_config = PassPhrase('./passphrase.toml')

ssh_config = Config(overrides={'sudo': {'password': user_config.password},
                               'user': user_config.username,
                               'connect_kwargs': {'password': user_config.password}})

conn = Connection('192.168.0.1', config=ssh_config)

@task
def create_user(c):
    passwd_response = Responder(pattern='', response='{}\n'.format(),)
    pass

@task
def remove_user(c):
    pass

@task
def whoami(c):
    conn.run('whoami')
    conn.sudo('whoami', hide='stderr')
