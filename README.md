# NPI

The NPI package manages known publicly-available data on physicians and other health care workers that is linked to their NPI number.


### Using the main download module

This downloads NPI/NPPES data from two sources: CMS and the NBER. NBER data is downloaded by variable, and downloads those specified in `constants.py`.

Some common uses of the module can be accessed at the command line:

```bash
npi download --source NPPES
npi process --source NPPES --update True
```

To process only one variable:

```bash
npi process --source NPPES --variable npideactdate --update True
```

To process everything except a list of variables:

```bash
npi process --source NPPES --update True --exclude npideactdate npireactdate 
```

### Using the package as an API

See an example notebook here:

[NPI Database Query Example](https://github.com/akilby/npi/blob/master/NPI%20Database%20Query%20Example.ipynb)


### Old

```bash
python -m npi.download.nppes
```

Running: pnamesuffix
Timestamp ID: 1592577025853898

Slurm ID: 12289429
Running: pgender
Timestamp ID: 1592577046590159

Slurm ID: 12289430
Running: porgnameoth
Timestamp ID: 1592577086947340

Slurm ID: 12289432
Running: entity
Timestamp ID: 1592577148199020

Slurm ID: 12289433
Running: ploccityname
Timestamp ID: 1592577228689550

Slurm ID: 12289438
Running: pmnameoth
Timestamp ID: 1592577329233012

Slurm ID: 12289441
