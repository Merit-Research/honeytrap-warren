from setuptools import setup, find_packages

setup(
    name="honeytrap_warren",
    version="0.1.2",
    packages=find_packages(),
    entry_points={
        'console_scripts': [
            'honeytrap-warren = honeytrap_warren.honeytrap_warren:main',
            'warren = warren.warren:main',
        ]
    }
)