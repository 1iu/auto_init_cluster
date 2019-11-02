#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
 @desc:
 @author: @1iu
 @contact: atliuning@gmail.com
 @date: 2019/11/1
"""
from fabric import SerialGroup, GroupResult
from fabric.exceptions import GroupException


def generate_hosts(providedHosts, providedHostnames):
    hosts = ''
    i = 0
    size = len(providedHosts)
    while i < size:
        hosts = hosts + '\n' + providedHosts[i] + '\t' + providedHostnames[i]
        i = i + 1
    return hosts


class GroupHelper(SerialGroup):

    def sudo(self, *args, **kwargs):
        results = GroupResult()
        excepted = False

        for cxn in self:
            try:
                results[cxn] = cxn.sudo(*args, **kwargs)
            except Exception as e:
                results[cxn] = e
                excepted = True
        if excepted:
            raise GroupException(results)
        return results

    def get(self, *args, **kwargs):
        results = GroupResult()
        excepted = False

        for cxn in self:
            try:
                results[cxn] = cxn.get(*args, **kwargs)
            except Exception as e:
                results[cxn] = e
                excepted = True
        if excepted:
            raise GroupException(results)
        return results

    def put(self, *args, **kwargs):
        results = GroupResult()
        excepted = False

        for cxn in self:
            try:
                results[cxn] = cxn.put(*args, **kwargs)
            except Exception as e:
                results[cxn] = e
                excepted = True
        if excepted:
            raise GroupException(results)
        return results