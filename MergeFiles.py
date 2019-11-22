



USE_VAR_LIST = ['taxcode', 'locline1','locline2','loccityname','locstatename','loczip']

allfiles=[]
for variable in USE_VAR_LIST:
	for year in range(2007, 2020):
		for month in range(1, 13):
			file_path_stub = '/work/akilby/npi/raw/p%s%s%s' % (variable,year,month)
			try:
				file_path = '%s.csv' % file_path_stub
				df = pd.read_csv(file_path)
				allfiles.concat(df)
				print('Combining Files')
			except FileNotFoundError:
				try:
					file_path2='%s.dta' % file_path_stub
					df2= pd.read_stata(file_path2)
					allfiles.concat(df2)
				except FileNotFoundError:
					print('Warning: data does not exist')	

