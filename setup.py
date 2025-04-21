from setuptools import setup, find_packages

setup(
    name='data_ingest_tool',
    version='0.1.0',
    author='Your Name',
    author_email='your.email@example.com',
    description='A tool for ingesting data from Box into Delta tables.',
    packages=find_packages(),
    install_requires=[
        'boxsdk',
        'pandas',
        'cerberus',
        'cerberus-python-client',
        'openpyxl',
        'pyspark',
        'delta-spark'
    ],
    python_requires='>=3.6',
)