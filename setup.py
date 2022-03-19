import re
from setuptools import setup

requirements, dependency_links = [], []
with open('requirements.txt') as f:
    for line in f.read().splitlines():
        if line.startswith('-e git+'):
            dependency_links.append(line.replace('-e git+', ''))
        else:
            requirements.append(line)

setup(install_requires=requirements,
        dependency_links=dependency_links)
