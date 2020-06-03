from distutils.core import setup

setup(name="SqlFileGenerator",
      version="1.0 beta",
      packages=["updater"],
      license="",
      install_requires=['mysqlsdfdf', 'openpyxl', 'numpy'],
      long_description=open("README.txt").read())
