#!/usr/bin/env python
# -*- coding: UTF-8 -*-
#
#       utils.py
#
#       Copyright (c) 2014 
#       Author: Claudio Driussi <claudio.driussi@gmail.com>
#
import os
import yaml


def load_yaml(fname, paths="."):
    """ Load a yaml file with includes and list of path search.
    Usage:
         data = load_yaml('foo.yaml', ['yaml','yaml/core'])
    paths is a list of paths where search files defined with !include role.
    """
    for p in paths:
        filename = os.path.join(p, fname)
        if os.path.isfile(filename):
            with open(filename, 'r') as f:
                return yaml.load(f, YamlLoader)
        raise Exception('File %s not found!' % fname)

class YamlLoader(yaml.Loader):
    """
    Load a yaml file with includes and list of path search.
    Usage:
         data = YamlLoader(open('foo.yaml','r'),paths).get_data()
         or
         data = yaml.load(open('foo.yaml','r',['yaml','yaml/core']), YamlLoader)

    paths is a list of paths where search files defined with !include role.
    """
    def __init__(self, stream, paths=['.']):
        self.paths = paths
        super(YamlLoader, self).__init__(stream)
        self.add_constructor('!include', YamlLoader.include)

    def include(self, node):
        for p in self.paths:
            filename = os.path.join(p, self.construct_scalar(node))
            if os.path.isfile(filename):
                with open(filename, 'r') as f:
                    return yaml.load(f, YamlLoader)
        raise Exception('Include file %s not found!' % self.construct_scalar(node))

