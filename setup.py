from setuptools import setup, find_packages

__version__ = '1.0.1'


setup(name='seatable-api',
      version=__version__,
      license='Apache Licence',
      description='Client interface for SeaTable Web API',
      author='seatable',
      author_email='support@seafile.com',
      url='https://github.com/seatable/seatable-api-python',

      platforms='any',
      packages=find_packages(),  # folder with __init__.py
      install_requires=['requests', 'socketIO-client-nexus'],
      classifiers=['Programming Language :: Python'],
      )
