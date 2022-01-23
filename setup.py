#!/usr/bin/python

from setuptools import setup

import distutils.command.install_scripts
import shutil
import os

if os.path.exists('README'):
    with open('README') as readme_file:
        long_description = readme_file.read().strip()
else:
    long_description = ''


# idea from http://stackoverflow.com/a/11400431/2139420
class StripExtension(distutils.command.install_scripts.install_scripts):
    """
    Class to handle the stripping of .py extensions in for executable file names
    making them more user friendly
    """
    def run(self):
        distutils.command.install_scripts.install_scripts.run(self)
        for script in self.get_outputs():
            if script.endswith(".py"):
                shutil.move(script, script[:-3])


setup(
    name="ceph_nvmeof",
    version="0.1",
    description="Common classes/functions and CLI tools used to configure NVMeoF "
                "gateways backed by Ceph RBD",
    long_description=long_description,
    url="http://github.com/ceph/ceph-nvmeof",
    license="GPLv3",
    packages=[
        "nvme_gw",
    ],
    scripts=[
        "nvme_gw_cli.py",
        'nvme_gw_server.py',
    ],
    cmdclass={
        "install_scripts": StripExtension
    }
)
