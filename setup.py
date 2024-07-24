import setuptools
import os

os.chdir(os.path.abspath(os.path.dirname(__file__)))
with open('requirements.txt') as f:
    requirements = f.read().splitlines()

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="battetl",
    version="1.1.16",
    author="Zander Nevitt, Bing Syuan Wang, Eric Ravet, Aditya Parsai and Chintan Pathak",
    author_email="info@battgenie.life",
    description="A Python module for extracting, transforming, and loading battery data to a database.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/BattGenie/battetl",
    packages=setuptools.find_packages(),
    install_requires=requirements,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.9, <3.12',
    license='MIT',
    include_package_data=True,
    entry_points={
        'console_scripts': [
            'battetl = battetl:run_battetl',
        ]
    }
)
