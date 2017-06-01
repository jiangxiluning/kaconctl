#!/bin/env python
# coding: utf8
# kafka-schema-registry.py ---
# 
# Filename: kafka-schema-registry.py
# Description: kafka avro schema 管理程序
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
'''kafka-schema-registry is a tool for avro schema in kafka.

Usage:
  kafka-schema-registry (-l | --list)
  kafka-schema-registry schema <subject> (-s | --show)
  kafka-schema-registry schema <subject> (-s | --show) [<id>]
  kafka-schema-registry schema <subject> (-r | --register) <schema.avsc>
  kafka-schema-registry compatibility (-s | --show) [<subject>]
  kafka-schema-registry compatibility --set <compatibility> [<subject>]
  kafka-schema-registry compatibility (-t | --test) <subject> <id> <schema.avsc>

Specification:
  -l --list      Get a list of registered subjects.
  -s --show      Get a list of versions registered under the specified subject.
  -r --register  Register a new schema under the specified subject.
  
'''

import requests
import json
import sys
import docopt
import pprint
from enum import IntEnum

SCHEMA_REGISTRY="http://da100:8083"

class COMPATIBILITY(IntEnum):
    NONE = 0
    BACKWARD = 1
    FORWARD = 2
    FULL =3

class Controller(object):

    def __init__(self, opts):
        self.subject = opts['<subject>']
        self.versionId = opts['<id>']
        self.compatibility = opts['<compatibility>']
        try:
            if opts['<schema.avsc>'] is not None:
                self.schema_avsc = json.load(open(opts['<schema.avsc>']))
        except:
            print('Cannot read configuration file.')
            sys.exit(-1)

        self.opts = opts

    def run_command(self):
        if self.opts['--list']:
            self.list_registered_subjects()
        elif self.opts['schema']:
            if self.opts['--show']:
                if self.opts['<id>']:
                    self.get_subject_scheme_of_version()
                else:
                    self.list_registered_versions_under_subject()
            elif self.opts['--register']:
                self.register_a_schema_under_subject()
        elif self.opts['compatibility']:
            if self.opts['--show']:
                if self.opts['<subject>']:
                    self.config_show_subject()
                else:
                    self.config_show()
            elif self.opts['--set']:
                if self.opts['<subject>']:
                    self.set_compatibility_subject()
                else:
                    self.set_compatibility()
            elif (self.opts['--test'] or self.opts['-t']):
                self.test_compatibility()

    def test_compatibility(self):
        url = '/compatibility/subjects/{0}/versions/{1}'.format(self.subject, self.versionId)
        self.update_url(url)
        self.request('post', data = {'schema': json.dumps(self.schema_avsc)})


    def update_url(self, url):
        self.url = SCHEMA_REGISTRY+url

    def request(self, method, data=None):
        try:
            if method == 'get':
                response = requests.get(
                    url=self.url,
                    headers={
                        "Accept": "application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, application/json",
                        "Content-Type": "application/vnd.schemaregistry.v1+json"
                    }
                )
            elif method == 'post':
                response = requests.post(
                    url=self.url,
                    headers={
                        "Accept": "application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, application/json",
                        "Content-Type": "application/vnd.schemaregistry.v1+json"
                    },
                    data = json.dumps(data)
                )
            elif method == 'put':
                response = requests.put(
                    url=self.url,
                    headers={
                        "Accept": "application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, application/json",
                        "Content-Type": "application/vnd.schemaregistry.v1+json"
                    },
                    data = json.dumps(data)
                )
            elif method == 'delete':
                response = requests.delete(
                    url=self.url
                )

            print(response.status_code)
            pprint.pprint(json.loads(response.content))
        except requests.exceptions.RequestException:
            print('HTTP Request failed')


    def list_registered_subjects(self):
        self.update_url("/subjects")
        self.request('get')

    def list_registered_versions_under_subject(self):
        url = "/subjects/{0}/versions".format(self.subject)
        self.update_url(url)
        self.request('get')

    def get_subject_scheme_of_version(self):
        url = "/subjects/{0}/versions/{1}".format(self.subject, self.versionId)
        self.update_url(url)
        self.request('get')

    def register_a_schema_under_subject(self):
        url = "/subjects/{0}/versions".format(self.subject)
        self.update_url(url)
        self.request('post', data = {'schema': json.dumps(self.schema_avsc)})

    def config_show(self):
        self.update_url("/config")
        self.request('get')

    def config_show_subject(self):
        url = "/config/{0}".format(self.subject)
        self.update_url(url)
        self.request('get')

    def set_compatibility(self):
        pass

    def set_compatibility_subject(self):
        data = {
            'compatibility': None
        }
        
        data['compatibility'] = self.compatibility

        url = "/config/{0}".format(self.subject)
        self.update_url(url)
        self.request('put', data=data)


if __name__ == '__main__':
    opts = docopt.docopt(__doc__)
    c = Controller(opts)
    c.run_command()


# 
# kafka-connect-ctl.py ends here
