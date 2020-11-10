import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="data-warehouse-client",
    version="0.0.3",
    author="Paul Watson",
    author_email="paul.watson@ncl.ac.uk",
    description="This package provides access to the e-Science Central data warehouse that can be used to store, "
                "access and analyse data collected in scientific studies, including for healthcare applications",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/e-science-central/data-warehouse-client",
    packages=["data_warehouse_client"],
    use_scm_version=True,
    setup_requires=['setuptools_scm'],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
    install_requires=[
        "more-itertools",
        "matplotlib",
        "psycopg2",
        "tabulate"
    ],
    python_requires='>=3.6',
)
