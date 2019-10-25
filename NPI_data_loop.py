import wget
import os
import urllib.error

RAW_DATA_DIR = '/work/akilby/npi/raw'
USE_VAR_LIST = ['taxcode', 'locline1','locline2','loccityname','locstatename','loczip']

if not os.path.isdir(RAW_DATA_DIR):
	os.makedirs(RAW_DATA_DIR)


download_fail_list = []
for variable in USE_VAR_LIST:
	for year in range(2007, 2020):
		for month in range(1, 13):
			download_path_stub = 'https://data.nber.org/npi/byvar/%s/%s/p%s%s%s' % (year, month, variable, year, month)
			try:
				download_path = '%s.csv' % download_path_stub
				destination_path =  os.path.join(RAW_DATA_DIR, wget.detect_filename(download_path))
				if not os.path.isfile(destination_path):
					print('WGetting csv download_path: %s' % download_path)
					wget.download(download_path, out=RAW_DATA_DIR)
				else:
					print('File already downloaded to: %s' % destination_path)
			except(urllib.error.HTTPError):
				try:
					download_path2 = '%s.dta' % download_path_stub
					destination_path2 =  os.path.join(RAW_DATA_DIR, wget.detect_filename(download_path2))
					if not os.path.isfile(destination_path2):
						print('WGetting dta download_path2: %s' % download_path2)
						wget.download(download_path2, out=RAW_DATA_DIR)
					else:
						print('File already downloaded to: %s' % destination_path2)
				except(urllib.error.HTTPError):
					download_fail_list.append(download_path_stub)
					print('Warning: data does not exist')


