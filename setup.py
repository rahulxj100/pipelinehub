from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="flexible-datapipeline",
    use_scm_version=False,
    version="0.1.0",
    author="Rahul Paul",
    author_email="paul.rahulxj100@gmail.com",
    description="A flexible data pipeline library for custom data processing workflows",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/rahulxj100/pipelinehub",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
    python_requires=">=3.7",
    install_requires=[],
    extras_require={
        "dev": ["pytest>=6.0", "black", "flake8", "mypy"],
    },
)
