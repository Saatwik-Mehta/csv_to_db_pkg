import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="csvtodb-converter-saatwikmehta",
    version="0.0.3",
    author="Saatwik Mehta",
    author_email="saatwikmehta@gmail.com",
    description="This package is useful for someone who wants to make changes inside his CSV files."
                "It creates a local server that provides the functionality of file upload to db"
                "Reading the data inside the db table created using the name of the file and "
                "applying CRUD operations as well."
                ,
    long_description=long_description,
    long_description_content_type="text/markdown",

    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    package_dir={"": "src"},
    packages=setuptools.find_packages(where="src"),
    python_requires=">=3.7",
)