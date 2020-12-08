import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="datajuicer-faberf", # Replace with your own username
    version="0.0.1",
    author="Fynn Firouz Faber",
    author_email="faberf@ethz.ch",
    description="A package for data manipulation and function execution.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="TODO",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)