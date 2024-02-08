from setuptools import setup, find_packages

setup(
    name='data_retriever',
    version='0.0.2',
    description='A library that contains method to retrieve data from third party APIs',
    author='Ioannis Tsakmakis, Nikolaos Kokkos',
    author_email='itsakmak@envrio.org, nkokkos@envrio.org',
    packages=find_packages(),
    python_requires='>=3.12',
    install_requires=[  
        'databases_utils>=1.1.0',
        'requests>=2.31.0',
        'xmltodict>=0.13.0',
        'pandas>=2.1.4'
    ],
    classifiers=[  
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3.12',
        'Framework :: Flask',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ]
)
