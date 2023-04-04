import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="battetl",
    version="1.0.0",
    author="Zander Nevitt, Bing Syuan Wang and Chintan Pathak",
    author_email="info@battgenie.life",
    description="A Python module for extracting, transforming, and loading battery data to a database.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/BattGenie/battetl",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: Linux",
    ],
    python_requires='>=3.9, <3.11',
)
