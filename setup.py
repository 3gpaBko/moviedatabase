from setuptools import setup, find_packages

setup(
    name='Adastra PY Challange',
    version='1.0.0',
    author='Zdravko Mavrov',
    author_email='mavrov.zdravko@gmail.com',
    description='Movies data set creation and manipulation',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/3gpaBko/moviedatabase.git',
    packages=find_packages(),
    install_requires=[
        # List of your dependencies
        'numpy',
        'pandas'
    ],
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
)
