import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

NAME = 'niriPipe'
PACKAGE_DATA = {
    '': ['*.dat', '*.cfg', '*.fits', '*.txt']
}

setuptools.setup(
    name=NAME,
    version="0.0.1",
    author="Nat Comeau",
    author_email="ncomeau@uvic.ca",
    description="A small example package",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/nat1405/niriPipe",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
    package_data=PACKAGE_DATA,
)
