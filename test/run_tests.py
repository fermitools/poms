#!/usr/bin/env python

import utils
import os

utils.setUpPoms()

os.system("python -m unittest discover -v")

utils.tearDownPoms()
