from setuptools import setup, find_packages

with open("requirements.txt") as f:
	install_requires = f.read().strip().split("\n")

# get version from __version__ variable in msacco_api/__init__.py
from msacco_api import __version__ as version

setup(
	name="msacco_api",
	version=version,
	description="API to centralize MSACCO transactions",
	author="Victor ABIZEYIMANA",
	author_email="svicky.shema@gmail.com",
	packages=find_packages(),
	zip_safe=False,
	include_package_data=True,
	install_requires=install_requires
)
