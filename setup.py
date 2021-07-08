# -*- coding: utf-8 -*-
from setuptools import setup, find_packages

with open('requirements.txt') as f:
	install_requires = f.read().strip().split('\n')

# get version from __version__ variable in adnconnect/__init__.py
from adnconnect import __version__ as version

setup(
	name='adnconnect',
	version=version,
	description='ADN',
	author='itsdave',
	author_email='dev@itsdave.de',
	packages=find_packages(),
	zip_safe=False,
	include_package_data=True,
	install_requires=install_requires
)
