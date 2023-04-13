import os
import setuptools
from pip._internal.req import parse_requirements
from pip._internal.network.session import PipSession

this_dir = os.path.dirname(os.path.abspath(__file__))
pip_requirements = parse_requirements(
    os.path.join(this_dir, "requirements.txt"), PipSession())
reqs = [pii.requirement for pii in pip_requirements]

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="battetl",
    version="1.0.1",
    author="Zander Nevitt, Bing Syuan Wang and Chintan Pathak",
    author_email="info@battgenie.life",
    description="A Python module for extracting, transforming, and loading battery data to a database.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/BattGenie/battetl",
    packages=setuptools.find_packages(),
    install_requires=reqs,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.9, <3.11',
    license='MIT',
)
