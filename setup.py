from setuptools import setup, find_packages

setup(
    name='Tools',                 # The name of your package
    version='0.1',
    packages=find_packages(),      # Automatically find the packages
    install_requires=[             # List of dependencies
        'PyPDF2',
        'filetype',
        'pymssql',
        'boto3',
        'datadog'
    ],
)
