from selenium import webdriver
from prompt_toolkit import prompt
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec
from bs4 import BeautifulSoup as bs
from rich.console import Console
from rich.table import Table
import sqlite3
import typing
import json
import time
import logging
import multiprocessing
import traceback
import sys
import os
import re

