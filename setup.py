import setuptools

with open('ReadMe.md', 'r') as f:
    long_description = f.read()

setuptools.setup(
    name="CIDataPortal", 
    version="1.0.0",
    author="Ayrton Bourn",
    author_email="AyrtonBourn@Outlook.com",
    description="Package for accessing the NG ESO Carbon Intensity API",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/AyrtonB/Carbon-Intensity-Data-Portal",
    packages=setuptools.find_packages(),
    package_data={'CIDataPortal':['*']},
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)