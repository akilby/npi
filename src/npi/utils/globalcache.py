from tributaries import cache
from tributaries.config import get_config_package

name = __name__.split('.')[0]
directory, noisily, rerun = get_config_package(name)

# Directory: '/work/akilby/npi/Cache/Caches'

c = cache.Cache(directory=directory,
                noisily=noisily,
                rerun=rerun,
                exclusion_list=[],
                registry=['npi.npi'],
                old_version=False)
