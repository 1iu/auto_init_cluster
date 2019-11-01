#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
 @desc:
 @author: @1iu
 @contact: atliuning@gmail.com
 @date: 2019/11/1
"""

import toml
import os
import logging


class ClusterConfig:

    def __init__(self, path):
        self.logger = logging.getLogger()
        if not os.path.exists(path):
            self.logger.error('no config in path: %s', path)
            raise FileNotFoundError
        self.config = toml.load(open(path, encoding='utf-8'))

    def get(self, k):
        if k in self.config:
            return self.config[k]
        else:
            self.logger.warning('no config with key: %s', k)
            return

    def set(self, k, v):
        self.config[k] = v


class PassPhrase:

    def __init__(self, path):
        self.logger = logging.getLogger()
        if not os.path.exists(path):
            self.logger.error('no passphrase in path: %s', path)
            raise FileNotFoundError
        config = toml.load(open(path, encoding='utf-8'))
        try:
            self.username = config['username']
            self.password = config['password']

        except KeyError as e:
            self.logger.error('Config key error: %s', e)
            raise e
