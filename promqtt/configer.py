'''Automatic configuration handling. Uses a config specification and generates a
dict structure with configuration values populated from command-line,
environment variables and possibly a configiration file.'''

import argparse
import os

def _set_struct(cfg, name, value, sep='.'):
    '''Parses a "path" string to locate the specified path in a dictionary to
    set a value.

    E.g. name='root.sub.val', value=foo => {'root': {'sub': {'val': 'foo'}}}'''

    parts = name.split(sep)

    # loop over all but last part
    for part in parts[:-1]:
        if part not in cfg:
            cfg[part] = {}

        cfg = cfg[part]

    cfg[parts[-1]] = value


def _get_struct(cfg, name, sep='.', default=None):
    '''Parses a "path" string to locate the specified path in a dictionary to
    get a value.

    E.g. name='root.sub.val', value=foo => {'root': {'sub': {'val': 'foo'}}}

    :param cfg: The configuration structure.
    :param name: The item to retrieve.
    :param set: The path separator ("." by default)
    :param default: The default value to return, if the specified path does not
      exist.'''

    parts = name.split(sep)

    for part in parts:
        if part in cfg:
            cfg = cfg[part]
        else:
            return default

    return cfg


def prepare_argparser(cfgdef, parser=None):
    '''Prepare an argument parser object (from argparse package) according to the
    configuration descriptor.

    :param cfgdef: The configuration descriptor.
    :param parser: Optional. Pass in an already created parser object. If this
      is ommited or None, a new parser object will be created.

    :returns: The created / updated parser object.'''

    if parser is None:
        parser = argparse.ArgumentParser()

    for name, info in cfgdef.items():
        parser.add_argument(
            '--' + name,
            required=False,
            default=None,
            help=info['help'],
            type=info['type'])

    return parser


def eval_args(cfgdef, cfg, args):
    '''Evaluate the passed in command-line arguments and update the configuration
    structure.

    :param cfgdef: The configuration descriptor.
    :param cfg: The configuration structure to update.
    :param args: The command-line parser result.'''

    argvars = vars(args)

    for name in cfgdef.keys():
        if (name in argvars) and (argvars[name] is not None):
            _set_struct(cfg, name, argvars[name])


def eval_env(cfgdef, cfg, env):
    '''Evaluate the passed in environment variables and update the configuration
    structure.

    :param cfgdef: The configuration descriptor.
    :param cfg: The configuration structure to update.
    :param env: The environment variables. This is usually `os.environ`.'''

    for name, info in cfgdef.items():
        varname = name.upper().replace('.', '_')
        if varname in env:
            _set_struct(cfg, name, info['type'](os.environ[varname]))


def eval_cfgfile_data(cfgdef, cfg, cfg_in):
    '''Evaluate the passed in configuration file data (or any other dict / list
    structure) and update the configuration structure.

    :param cfgdef: The configuration descriptor.
    :param cfg: The configuration structure to update.
    :param cfg_in: The configuration file data.'''

    for name in cfgdef.keys():
        val = _get_struct(cfg_in, name)
        if val is not None:
            _set_struct(cfg, name, val)


def eval_cfg(cfgdef, cfg_in, env, args):
    '''Evaluate the passed in configuration data from config file, environment
    variables and command-line (or any other dict / list structure) and update
    the configuration structure.

    :param cfgdef: The configuration descriptor.
    :param cfg_in: The configuration file data.
    :param env: The environment variables. This is usually `os.environ`.
    :param args: The command-line parser result.

    :returns: The configuration structure.'''

    cfg = {}

    for name, item in cfgdef.items():
        _set_struct(cfg, name, item['default'])

    eval_cfgfile_data(cfgdef, cfg, cfg_in)
    eval_env(cfgdef, cfg, env)
    eval_args(cfgdef, cfg, args)

    return cfg
