from setuptools import setup, find_packages

setup(name='subir',
      version='0.0.1',
      description='Xyla\'s script for uploading custom data.',
      url='https://github.com/xyla-io/subir',
      author='Xyla',
      author_email='gklei89@gmail.com',
      license='MIT',
      packages=find_packages(),
      install_requires=[
        'data_layer'
      ],
      zip_safe=False)