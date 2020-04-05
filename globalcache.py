from cache import cache
from cache.config import config_path, load_config_package


noisily, rerun = load_config_package(config_path(name='claims_data'))
c = cache.Cache(noisily=noisily, rerun=rerun)
