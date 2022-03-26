import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    version="0.0.4",
    name="datajuicer", # Replace with your own username
    author="Fynn Firouz Faber",
    author_email="faberf@ethz.ch",
    description="parallelism, caching and synchronization for dynamic and complex computations",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/faberf/datajuicer/archive/v0.0.4.tar.gz",
    packages=["datajuicer"],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
    install_requires=[
   'dill', "portalocker"
    ],

)
