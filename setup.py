import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="data-warehouse-client",
    version="1.0.0",
    author="Paul Watson",
    author_email="benwatson528@gmail.com",
    description="An example of uploading to PyPi",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/e-science-central/data-warehouse-client",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=[
        "tabulate"
    ],
    python_requires='>=3.6',
)
