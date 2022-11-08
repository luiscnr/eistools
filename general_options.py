import os
import configparser


def get_options_from_file():
    config_file = 'eistools.config'
    if os.path.exists(config_file) and os.path.isfile(config_file):
        options = configparser.ConfigParser()
        options.read(config_file)
        return options
    else:
        return None


def get_param(section, option):
    options = get_options_from_file()
    if options is None:
        return None
    if options.has_option(section, option):
        return options[section][option]
    else:
        return None


def use_dt_suffix():
    op = get_param('GENERAL', 'use_dt_suffix')
    if op is None:
        return False
    op = op.strip().lower()
    if op == '1' or op == 'yes':
        return True
    else:
        return False


def get_eis():
    return get_param('GENERAL', 'eis')
