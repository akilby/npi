from tributaries import cache

# from tributaries.config import get_config_package

# name = __name__.split('.')[0]
# directory, noisily, rerun = get_config_package(name)

c = cache.Cache(directory='/work/akilby/npi/Cache/Caches',
                noisily=True,
                rerun=False,
                exclusion_list=[],
                registry=['npi.npi',
                          'npi.pecos',
                          'npi.process.samhsa',
                          'npi.process.physician_compare'],
                old_version=False)
