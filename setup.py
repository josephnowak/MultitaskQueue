from setuptools import setup, find_packages


with open('requirements.txt') as f:
    required = f.read().splitlines()

setup(
    name='multitask_queue',
    version='0.01',
    description='Framework to organize historical tasks useful for backtesting',
    author='Joseph Nowak',
    author_email='josephgonowak97@gmail.com',
    classifiers=[
        'Development Status :: 1 - Beta',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'Intended Audience :: General',
        'Natural Language :: English',
        'Programming Language :: Python :: 3.8',
    ],
    keywords='Framework Organizer Historical Backtest',
    packages=find_packages(),
    install_requires=required
)