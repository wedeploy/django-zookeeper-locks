import os
from setuptools import setup


with open(os.path.join(os.path.dirname(__file__), 'README.md')) as readme:
    README = readme.read()

with open('requirements.txt') as requirements:
    required = requirements.read().splitlines()


def get_packages(package):
    """
    Return root package and all sub-packages.
    """
    return [
        dirpath for dirpath, dirnames, filenames in os.walk(package)
        if os.path.exists(os.path.join(dirpath, '__init__.py'))]


# allow setup.py to be run from any path
os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))

setup(
    name='django-zookeeper-locks',
    version='1.0.0',
    packages=get_packages('zookeeper_locks'),
    include_package_data=True,
    description='Utils for using Zookeeper locks through zookeeper in Django commands and views.',
    long_description=README,
    long_description_content_type='text/markdown',
    author='WeDeploy',
    author_email='github@wedeploy.com',
    install_requires=required,
    tests_require=[
        'mock',
    ],
    test_suite='testproject.runtests.run_tests',
    classifiers=[
        'Environment :: Web Environment',
        'Framework :: Django',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
    ],
)
