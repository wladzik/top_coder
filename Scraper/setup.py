from distutils.core import setup

setup(name="HPPrintersScraper",
      version="1.0 beta",
      packages=["scraper"],
      license="",
      install_requires=['grequests', 'requests', 'openpyxl'],
      long_description=open("README.txt").read())
