from setuptools import setup

setup(
    name='dpostools',
    version='0.1.1',
    packages=['dpostools', 'dpostools.tests', 'park', 'park.api', 'park.builder'],
    url='https://github.com/BlockHub/blockhubdpostools.git',
    license='MIT',
    author='Charles',
    author_email='karel@blockhub.nl',
    description='generic toolkit for interacting with DPOS chains',
    include_package_data=True,
    package_data={'': ['yamls/*.yaml']},
    install_requires=[
          'pyyaml', 'psycopg2-binary', 'arky', 'wheel', 'Naked', 'Jinja2', 'requests',
          'MarkupSafe', 'certifi', 'urllib3', 'chardet', 'idna'
      ],
    dependency_links=['https://github.com/faustbrian/ARK-Python/archive/master.zip']
)
