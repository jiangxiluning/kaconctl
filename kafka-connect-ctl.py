#!/bin/env python
# coding: utf8
# kafka-connect-ctl.py ---
# 
# Filename: kafka-connect-ctl.py
# Description: kafka connect 控制程序
# Author:  Lu Ning
# Maintainer: Lu Ning
# Created: Thu May 25 11:17:28 2017 (+0800)
# Version: 
# Package-Requires: ()
# Last-Updated: 
#           By: 
#     Update #: 0
# URL: 
# Doc URL: 
# Keywords: 
# Compatibility: 
# kafka 0.10.0.1
# confluent-platform v3.2.1

# Commentary: 
# 
# 
# 
# 

# Change Log:
# 
# 
# 

# Code:
'''kafka-connect-ctl is a tool for deploying connectors into our clusters. And it can be used
to update config of a connector, etc. It implements all rest apis of the distributed worker.

Usage:
  kafka-connect-ctl (-l | --list) [--tasks]
  kafka-connect-ctl <connector_name> --create <config.json>
  kafka-connect-ctl <connector_name> (-i | --info)
  kafka-connect-ctl <connector_name>  --list-config
  kafka-connect-ctl <connector_name>  --update-config <config.json>
  kafka-connect-ctl <connector_name>  (-s | --status) [<task_id>]
  kafka-connect-ctl <connector_name>  --restart [<task_id>]
  kafka-connect-ctl <connector_name>  (--pause | --resume)
  kafka-connect-ctl <connector_name>  (-d | --delete)
  kafka-connect-ctl (-p | --plugins) [--validate=<plugin_class>]
'''

import requests
import json
import sys
import docopt

DISTRIBUTED_WORKER_INTERFACE="http://da100:18084"

class Controller(object):

    def __init__(self, opts):
        
        if opts.has_key('<connector_name>'):
            self.connector = opts['<connector_name>']
        if opts.has_key('<task_id>'):
            self.task_id = opts['<task_id>']
        if opts.has_key('<plugin_class>'):
            self.plugin_class = opts['<plugin_class>']

        try:
            if  opts['<config.json>'] is not None:
                self.config = json.load(open(opts['<config.json>']))
        except:
            print('Cannot read configuration file.')
            sys.exit(-1)

        self.opts = opts

    def run_command(self):
        if self.opts['--list'] or self.opts['-l']:
            if self.opts['--tasks']:
                pass
            else:
                self.list_active_connectors()
        elif self.opts['<connector_name>']:

            if self.opts['--update-config']:
                self.update_connector_config()
            elif self.opts['--restart']:

                if self.opts['<task_id>']:
                    self.restart_connector_tasks()
                else:
                    self.restart_connector()
            elif self.opts['--create']:
                self.create_connector()
            elif self.opts['--status']:
                if self.opts['<task_id>']:
                    self.status_connector_task()
                else:
                    self.status_connector()

            elif (self.opts['--info'] or self.opts['-i']):
                self.info_connector()
            elif (self.opts['-d'] or self.opts['--delete']):
                self.delete_connector()


    def delete_connector(self):
        url = '/connectors/{0}/'.format(self.connector)
        self.update_url(url)
        self.request('delete')


    def info_connector(self):
        url = '/connectors/{0}'.format(self.connector)
        self.update_url(url)
        self.request('get')

    def status_connector(self):
        url = '/connectors/{0}/status'.format(self.connector)
        self.update_url(url)
        self.request('get')

    def status_connector_task(self):
        url = '/connectors/{0}/tasks/{1}/status'.format(self.connector, self.task_id)
        self.update_url(url)
        self.request('get')

    def create_connector(self):
        url = '/connectors'
        self.update_url(url)
        self.request('post', data = {"name": self.connector,
                                     "config": self.config})


    def update_url(self, url):
        self.url = DISTRIBUTED_WORKER_INTERFACE+url

    def request(self, method, data=None):
        try:
            if method == 'get':
                response = requests.get(
                    url=self.url,
                    headers={
                        "Accept": "application/json"
                    }
                )
            elif method == 'post':
                response = requests.post(
                    url=self.url,
                    headers={
                        "Accept": "application/json",
                        "Content-Type": "application/json; charset=utf-8"
                    },
                    data = json.dumps(data)
                )
            elif method == 'put':
                response = requests.put(
                    url=self.url,
                    headers={
                        "Accept": "application/json",
                        "Content-Type": "application/json; charset=utf-8"
                    },
                    data = json.dumps(data)
                )
            elif method == 'delete':
                response = requests.delete(
                    url=self.url
                )

            print(response.status_code)
            print(response.content)
        except requests.exceptions.RequestException:
            print('HTTP Request failed')


    def list_active_connectors(self):
        self.update_url("/connectors")
        self.request('get')

    def update_connector_config(self):
        url = "/connectors/{0}/config".format(self.connector)
        self.update_url(url)
        self.request('put', data = self.config)

    def restart_connector(self):
        url = "/connectors/{0}/restart".format(self.connector)
        self.update_url(url)
        self.request('post', data={})

    def restart_connector_tasks(self):
        url = "/connectors/{0}/tasks/{1}/restart".format(self.connector, self.task_id)
        self.update_url(url)
        self.request('post', data={})

if __name__ == '__main__':
    opts = docopt.docopt(__doc__)
    c = Controller(opts)
    c.run_command()


# 
# kafka-connect-ctl.py ends here
