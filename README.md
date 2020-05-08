# NPI

The NPI package manages known publicly-available data on physicians and other health care workers that is linked to their NPI number.


### Using the main download module

This downloads NPI/NPPES data from two sources: CMS and the NBER. NBER data is downloaded by variable, and downloads those specified in `constants.py`.

```bash
python -m npi.download.nppes
```

### Using the package as an API

See an example notebook here:

[NPI Database Query Example](https://github.com/akilby/npi/blob/master/NPI%20Database%20Query%20Example.ipynb)
