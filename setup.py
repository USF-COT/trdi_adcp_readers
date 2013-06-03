from distutils.core import setup

setup(name='trdi_adcp_readers',
      version='1.0',
      py_modules=[
          'trdi_adcp_readers.pd15.pd0_converters',
          'trdi_adcp_readers.pd0.pd0_parser'
      ])
