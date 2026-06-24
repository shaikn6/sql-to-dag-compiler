from setuptools import setup, find_packages

setup(
    name="sql-to-dag-compiler",
    version="1.0.0",
    description="Converts Oracle SQL/PLSQL stored procedures into Apache Airflow 2.x DAGs",
    author="Nagizaaz Shaik",
    author_email="nagizaazs@gmail.com",
    url="https://github.com/shaikn6/sql-to-dag-compiler",
    packages=find_packages(),
    include_package_data=True,
    package_data={
        "sql_to_dag": ["templates/*.j2"],
    },
    install_requires=[
        "sqlparse==0.5.4",
        "Jinja2==3.1.6",
        "networkx==2.7.1",
    ],
    python_requires=">=3.9",
    classifiers=[
        "Programming Language :: Python :: 3.9",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    entry_points={
        "console_scripts": [
            "sql2dag=sql_to_dag.generator:main",
        ],
    },
)
