from setuptools import find_packages, setup

setup(
    name='django_stored_procedures',
    version='0.3.0',
    keywords=['django', 'stored procedures', 'database'],
    packages=find_packages(),
    url='https://github.com/derfenix/django_stored_procedures/',
    license='GPLv3+',
    author='Sergey Kostyuchenko',
    author_email='derfenix@gmail.com',
    description='',
    install_requires=['django>=1.8'],
    classifiers=[
        'License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)',
        'Development Status :: 5 - Production/Stable',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Framework :: Django',
        'Framework :: Django :: 1.8',
        'Framework :: Django :: 1.9',
        'Framework :: Django :: 1.10',
    ]
)
