from setuptools import setup, find_packages

__version__ = '0.1.6'


setup(name='seatable-api',
      version=__version__,
      license='Apache Licence',
      description='Client interface for SeaTable Web API',
      author='dzmbbs',
      author_email='dzmbbs@qq.com',
      url='https://github.com/seatable/seatable-api-python',

      platforms='any',
      packages=find_packages(),  # folder with __init__.py
      install_requires=['requests', 'socketIO-client-nexus'],
      classifiers=['Programming Language :: Python'],
      )
