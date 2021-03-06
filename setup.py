import os

from setuptools import setup


# Utility function to read the README file.
# Used for the long_description.  It's nice, because now 1) we have a top level
# README file and 2) it's easier to type in the README file than to put a raw
# string in below ...
def read(fname):
	return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
	name="fileshovel",
	version="0.1",
	author="Jérôme Poulin",
	author_email="jeromepoulin@gmail.com",
	license="GPL-3",
	url="https://github.com/ticpu/fileshovel",
	packages=['fileshovel', 'tests'],
	classifiers=[
		"Development Status :: 3 - Alpha",
		"Topic :: Utilities",
	],
)
