#
#  Copyright (c) 2021 International Business Machines
#  All rights reserved.
#
#  SPDX-License-Identifier: LGPL-3.0-or-later
#
#  Authors: anita.shekar@ibm.com, sandy.kaur@ibm.com
#

import os
import configparser
import logging


class NVMeGWConfig:
    def __init__(self, gw_config_filename):
        logging.basicConfig(level=logging.DEBUG)
        self.logger = logging.getLogger()
        if not os.path.isfile(gw_config_filename):
            self.logger.error(f"Config file {gw_config_filename} not found.")
            raise FileNotFoundError
        self.nvme_gw_config = configparser.ConfigParser()
        self.nvme_gw_config.read(gw_config_filename)

    def get(self, section, param, value=None):
        if value is None:
            return self.nvme_gw_config.get(section, param)
        else:
            return self.nvme_gw_config.get(section, param, fallback=value)

    def getboolean(self, section, param, value=None):
        if value is None:
            return self.nvme_gw_config.getboolean(section, param)
        else:
            return self.nvme_gw_config.getboolean(section, param, fallback=value)

    def getint(self, section, param, value=None):
        if value is None:
            return self.nvme_gw_config.getint(section, param)
        else:
            return self.nvme_gw_config.getint(section, param, fallback=value)

    def getfloat(self, section, param, value=None):
        if value is None:
            return self.nvme_gw_config.getfloat(section, param)
        else:
            return self.nvme_gw_config.getfloat(section, param, fallback=value)
