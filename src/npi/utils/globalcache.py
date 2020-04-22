from cache import cache
from cache.config import config_path, load_config_package

noisily, rerun = load_config_package(config_path(name='npi'))
c = cache.Cache(noisily=noisily,
                rerun=rerun)


# If cache installation is being used for multiple purposes, need
# a separate configuration file
# c = cache.Cache(noisily=noisily,
#                 rerun=rerun,
#                 configure='/work/akilby/npi/Cache/cache_config.txt')
