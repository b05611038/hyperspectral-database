import sys

if sys.version_info < (3, 6, ):
    raise RuntimeError('The coffee_db library do not support Python 2.X')

__version__ = '0.0.1'

from .database import HyperspectralDatabase

 
