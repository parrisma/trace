#!/usr/bin/env python
from setuptools import setup, find_packages

setup_info = dict(
    name='rltrace',
    version='1.0.0',
    author='Mark Parris',
    author_email='parris3141@gmail.com',
    download_url='http://pypi.python.org/pypi/rltrace',
    project_urls={
        'Source': 'https://github.com/parrisma/trace',
        'Tracker': 'https://github.com/parrisma/trace/issues',
    },
    description='Simple python logging with option to log direct to elastic search',
    long_description='',
    license='MIT',
    classifiers=[
        'Development Status :: 1 - Production/Stable',
        'Environment :: Win32 (MS Windows)',
        'Intended Audience :: Developers',
        'Operating System :: Microsoft :: Windows',
        'Programming Language :: Python :: 3.8',
        'Topic :: Education',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],

    # Package info
    packages=['rltrace'] + ['rltrace.' + pkg for pkg in find_packages('rltrace')],

    # Add _ prefix to the names of temporary build dirs
    options={'build': {'build_base': '_build'}, },
    zip_safe=True,
)

setup(**setup_info)
