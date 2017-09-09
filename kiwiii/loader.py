
import glob
import os
import yaml


try:
    with open("server_config.yaml") as f:
        config = yaml.load(f.read())
except FileNotFoundError:
    """ use server_config stub"""
    config = {
        "web_home": "",
        "basic_auth_realm": "",
        "user": {},
    }


def web_home_path():
    return config["web_home"]


def basic_auth_realm():
    return config["basic_auth_realm"]


def user_passwd_matched(user, passwd):
    users = config["user"]
    return user in users and passwd == users[user]['password']


def sqlite_list():
    pathname = os.path.join(config.get("sqlite_base_dir", ""), "*.sqlite3")
    return glob.glob(pathname)


def sqlite_path(db):
    return os.path.join(
        config.get("sqlite_base_dir", ""), "{}.sqlite3".format(db)
    )


def api_list():
    pathname = os.path.join(config.get("api_base_dir", ""), "*.yaml")
    return glob.glob(pathname)


def report_tmpl_list():
    pathname = os.path.join(config.get("report_template_dir", ""), "*.csv")
    return glob.glob(pathname)


def report_tmpl_file(filename):
    return os.path.join(config.get("report_template_dir", ""), filename)


def screener_api():
    # TODO: deprecated - API URL should be given by resource scheme
    return config.get("screener_api", "")
