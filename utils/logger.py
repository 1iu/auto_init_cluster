#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
 @desc:
 @author: @1iu
 @contact: atliuning@gmail.com
 @date: 2019/11/1
"""
import logging


class BookLogger:

    def __init__(self, debug_mode: bool):
        self.logger = logging.getLogger()
        log_format = logging.Formatter(
            fmt='%(asctime)s %(levelname)s %(filename)s[line:%(lineno)d] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
        )
        if debug_mode is True:
            self.logger.setLevel(logging.DEBUG)
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            console_handler.setFormatter(log_format)
            self.logger.addHandler(console_handler)
        else:
            self.logger.setLevel(logging.INFO)
            file_handler = logging.FileHandler('auto_init.log')
            file_handler.setLevel(logging.INFO)
            file_handler.setFormatter(log_format)
            self.logger.addHandler(file_handler)
