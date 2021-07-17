from setuptools import setup, find_packages

__version__ = ''

with open('README.md', 'r', encoding='utf-8') as fh:
    long_description = fh.read()

setup(
    name='seatable-api',
    version=__version__,
    license='Apache Licence',
    description='Python client for SeaTable web api',
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='seatable',
    author_email='support@seafile.com',
    url='https://github.com/seatable/seatable-api-python',

    platforms='any',
    packages=find_packages(),  # folder with __init__.py
    install_requires=['requests', 'python-socketio<5', 'ply', 'python_dateutil'],
    classifiers=['Programming Language :: Python'],
)
