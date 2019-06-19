import pathlib
import setuptools

HERE = pathlib.Path(__file__).parent

README = (HERE / 'README.md').read_text()

with open('requirements.txt') as fid:
    requires = [line.strip() for line in fid]

setuptools.setup(
    name="milvus_celery",
    version="0.0.1",
    description="Milvus Celery",
    long_description=README,
    long_description_content_type='text/markdown',
    url='http://192.168.1.105:6060/peng.xu/milvus-celery.git',
    license="Apache-2.0",
    author='Xu Peng',
    author_email='xupeng3112@163.com',
    packages=['milvus_celery'],
    include_package_data=True,
    install_requires=requires,
    classifiers=[
        "Programming Language :: Python :: 3.6",
    ],

    python_requires='>=3.6'
)
