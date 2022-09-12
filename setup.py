from setuptools import setup, find_packages

setup_info = dict(
    name='rltrace',
    version='1.0.2',
    author='Mark Parris',
    author_email='parris3141@gmail.com',
    download_url='https://pypi.org/project/rltrace/1.0.0/',
    project_urls={
        'Source': 'https://github.com/parrisma/trace',
        'Tracker': 'https://github.com/parrisma/trace/issues',
    },
    description='Simple python logging with option to log direct to elastic search',
    long_description='',
    license='MIT',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Environment :: Win32 (MS Windows)',
        'Intended Audience :: Developers',
        'Operating System :: Microsoft :: Windows',
        'Programming Language :: Python :: 3.8',
        'Topic :: Education',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],

    extras_require={'numpy': ['numpy~=1.19.5'],
                    'python-kubernetes': ['python-kubernetes~=24.2.0'],
                    'pytz': ['pytz~=2021.1'],
                    'elasticsearch': ['elasticsearch~=8.3.1'],
                    'setuptools': ['setuptools~=52.0.0']},

    # Package info
    packages=['rltrace'] + ['rltrace.' + pkg for pkg in find_packages('rltrace')],

    # Add _ prefix to the names of temporary build dirs
    options={'build': {'build_base': '_build'}, },
    zip_safe=True,
)

setup(**setup_info)
