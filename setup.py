from setuptools import setup, find_packages

setup(
    name='tsdbutils',
    version='0.2.0',
    packages=find_packages(),
    description='A utility library for working with time-series databases.',
    author='Simon Fester',
    author_email='syhester@gmail.com',
    url='https://github.com/simonfester/tsdbutils',
    install_requires=[
        # List your project's dependencies here.
        # Examples:
        # 'requests>=2.25.1',
        # 'pandas>=1.2.3',
    ],
)
