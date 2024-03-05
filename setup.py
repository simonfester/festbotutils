from setuptools import setup, find_packages

setup(
    name='tsdbutils',
    version='0.2.0',
    packages=find_packages(),
    description='A utility library for working with time-series databases.',
    author='Simon Fester',
    author_email='syhester@gmail.com',
    url='https://github.com/simonfester/tsdbutils',
    py_modules=['tsdbutils'],
)
