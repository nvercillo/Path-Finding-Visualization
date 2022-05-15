#! /usr/bin/python3

import logging
import sys
logging.basicConfig(stream=sys.stderr)
sys.path.insert(0, '/home/snvercil/stefan/Path-Finding-Visualization')
from flaskapp import app as application
#application.secret_key = "asghs89dfa5025awe9907"