from setuptools import setup

setup(
    name='dpostools',
    version='0.0.3',
    packages=['dpostools', 'dpostools.tests'],
    ext_package='arkdbtools',
    url='https://github.com/BlockHub/blockhubdpostools.git',
    license='MIT',
    author='Charles',
    author_email='karel@blockhub.nl',
    description='generic toolkit for interacting with DPOS chains'
)
