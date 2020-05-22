from setuptools import find_packages, setup

setup(name='npi',
      version='0.1',
      description=('NPI data management: download, process, clean'),
      url='http://github.com/akilby/npi',
      author='Angela E. Kilby',
      author_email='a.kilby@northeastern.edu',
      license='MIT',
      packages=find_packages('src'),
      include_package_data=True,
      package_dir={'': 'src'},
      install_requires=['pandas'],
      zip_safe=False,
      entry_points={
        'console_scripts': [
            'npi = npi.__main__:main',
        ],
      }
      )
