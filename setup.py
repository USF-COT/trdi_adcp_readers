from distutils.core import setup

setup(
    name='trdi_adcp_readers',
    version='1.0',
    py_modules=[
        'trdi_adcp_readers.readers',
        'trdi_adcp_readers.pd0.pd0_parser',
        'trdi_adcp_readers.pd15.pd0_converters'
    ],
    scripts=['scripts/convert_trdi.py'],
    description='A library capable of reading various TRDI ADCP '
                'data formats including PD15 and PD0 into python '
                'native types.',
    author='USF CMS Ocean Technology Group',
    maintainer='Michael Lindemuth',
    maintainer_email='mlindemu@usf.edu',
    url='https://github.com/USF-COT/trdi_adcp_readers',
    license='MIT',
    long_description=open("README.md").read()
)
