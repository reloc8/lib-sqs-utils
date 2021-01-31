import os
import setuptools

from typing import AnyStr


GITHUB_PERSONAL_ACCESS_TOKEN = os.getenv('GITHUB_PERSONAL_ACCESS_TOKEN')


def private_dependency(personal_access_token: AnyStr,
                       repo_user: AnyStr, repo_name: AnyStr,
                       package_name: AnyStr, package_version: AnyStr):
    """Defines a dependency from a private Github repository

    :param personal_access_token:   Github Personal Access Token
    :param repo_user:               Dependency repository user
    :param repo_name:               Dependency repository name
    :param package_name:            Dependency package name
    :param package_version:         Dependency repository release (tag)
    :return:                        The dependency specification for the install_requires field
    """

    return f'{package_name} @ ' \
           f'git+https://{personal_access_token}@github.com/' \
           f'{repo_user}/{repo_name}.git/@{package_version}#egg={package_name}-0'


with open('version', 'r') as version:

    setuptools.setup(
        name='sqs_utils',
        version=version.readline(),
        author='Alessio Vierti',
        packages=setuptools.find_packages(exclude=['tests']),
        install_requires=[
            'boto3==1.16.53',
            'botocore==1.19.54'
        ],
        python_requires='>=3.6'
    )
