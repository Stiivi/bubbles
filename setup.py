from setuptools import setup, find_packages

install_requires = []

setup(
    name = "bubbles",
    version = '0.1',

    # Project uses reStructuredText, so ensure that the docutils get
    # installed or upgraded on the target machine
    install_requires=install_requires,

    packages = find_packages(exclude=["*.tests", "*.tests.*", "tests.*", "tests"]),

    package_data={
        # If any package contains *.txt or *.rst files, include them:
        '': ['*.txt', '*.rst'],
    },

    scripts=[],

    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Topic :: Database',
        'Topic :: Scientific/Engineering',
        'Topic :: Utilities'
    ],

    test_suite="tests",

    # metadata for upload to PyPI
    author="Stefan Urbanek",
    author_email="stefan.urbanek@gmail.com",
    description="Virtual Data Object framework for data processing (ETL) and quality monitoring",
    license="MIT",
    keywords="data analysis quality datamining",
    url="http://bubbles.databrewery.org"
)
