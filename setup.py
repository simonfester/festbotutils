from setuptools import setup, find_packages

setup(
    name='festbotutils',
    version='0.5.1',
    packages=find_packages(),
    description='A utility library for working with time-series databases.',
    author='Simon Fester',
    author_email='syhester@gmail.com',
    url='https://github.com/simonfester/festbotutils',
    py_modules=['tsdbutils','utils','datautils'],
)
