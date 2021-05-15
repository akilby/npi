from tributaries import cache
from tributaries.config import config_path, get_config_package

name = __name__.split('.')[0]
print(name)
print(config_path(name=name))
directory, noisily, rerun = get_config_package(config_path(name=name))

# Directory: '/work/akilby/npi/Cache/Caches'

c = cache.Cache(configure={'directory': directory,
                           'registry': [__name__],
                           'exclusion_list': []},
                noisily=noisily,
                rerun=rerun,
                old_version=False)
