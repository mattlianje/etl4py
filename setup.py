from setuptools import setup

setup(
   name="etl4py",
   version="0.1.0",
   py_modules=["etl4py"],
   python_requires=">=3.7",
   author="Matthieu Court",
   author_email="matthieu.court@protonmail.com",
   description="Beautiful, typesafe dataflows that scale from laptop to thousands of cores",
   long_description=open("README.md").read(),
   long_description_content_type="text/markdown",
   url="https://github.com/mattlianje/etl4py",
   license="GPL-3.0",
   classifiers=[
       "Programming Language :: Python :: 3.7",
       "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
   ],
)
