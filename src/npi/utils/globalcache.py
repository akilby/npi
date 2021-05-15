from tributaries import cache
from tributaries.config import config_path, load_config_package

print(__name__.split('.')[0])
noisily, rerun = load_config_package(config_path(name='npi'))


c = cache.Cache(configure={'directory': '/work/akilby/npi/Cache/Caches',
                           'registry': [__name__],
                           'exclusion_list': []},
                noisily=noisily,
                rerun=rerun,
                old_version=False)

# If cache installation is being used for multiple purposes, need
# a separate configuration file
# c = cache.Cache(noisily=noisily,
#                 rerun=rerun,
#                 configure='/work/akilby/npi/Cache/cache_config.txt')
