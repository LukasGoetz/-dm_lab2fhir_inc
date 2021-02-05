#!/usr/bin/python3.6

import pytest, logging, configparser
from lib import umm_db_lib

def pytest_addoption(parser):
  parser.addoption('--type', action='store')
  parser.addoption('--app', action='store')
  parser.addoption('--cfile', action='store')
  parser.addoption('--db', action='store')

def pytest_report_header(config):
  info_text = []
  if config.getoption("type") == 'unit_test':
    info_text = ["******Unit Tests******",
                 "Check all basic class methods giving valid/ invalid data"]
  elif config.getoption("type") == 'int_test':
    info_text = ["******Integration Tests******",
                 "Check all complex class methods giving valid/ invalid data"]
  elif config.getoption("type") == 'sys_test':
    info_text = ["******System Test******",
                 "Check complete application giving valid data"]
  return info_text