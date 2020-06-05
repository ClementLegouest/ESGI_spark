from setuptools import setup, find_packages

setup(
    name="FootballStatistics",
    version="0",
    packages=find_packages(),

    # Project uses reStructuredText, so ensure that the docutils get
    # installed or upgraded on the target machine
    install_requires=["docutils>=0.3"],
    package_data={
    # If any package contains *.txt or *.rst files, include them:
    "": ["*.txt", "*.rst"],
    # And include any *.msg files found in the "hello" package, too:
    # "hello": ["*.msg"],
    },

    # metadata to display on PyPI
    author="Clément Legouest",
    author_email="clement.legouest@gmail.com",
    description="Une application calculant des statistiques sur les résultats de l'équipe de France de football",

    # could also include long_description, download_url, etc.
)