from distutils.core import setup

setup(
    name='trdi_adcp_readers',
    version='1.0',
    py_modules=[
        'trdi_adcp_readers.pd15',
        'trdi_adcp_readers.pd0'
    ],
    description='A library capable of reading various TRDI ADCP '
                'data formats including PD15 and PD0 into python '
                'native types.',
    author='USF CMS Ocean Technology Group',
    maintainer='Michael Lindemuth',
    maintainer_email='mlindemu@usf.edu',
    url='https://github.com/USF-COT/trdi_adcp_readers',
    license='Apache',
    long_description=open("README.md").read()
)
