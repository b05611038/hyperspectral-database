import sys

if sys.version_info < (3, 6, ):
    raise RuntimeError('The coffee_db library do not support Python 2.X')

__version__ = '1.1.0'

from .database import HyperspectralDatabase

 
