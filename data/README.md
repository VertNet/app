# Overview

This directory contains tools for working with data.

# Darwin Core Archives → CartoDB

Right now the process of converting a [Darwin Core Archive](http://www.gbif.org/informatics/standards-and-tools/publishing-data/data-standards/darwin-core-archives) to CartoDB is a leetle painful. This will be automated down the road,  but for now here are the steps:

1.  Convert Darwin Core Archive to Darwin Core CSV file using `dwca2csv.py`
2.  Upload CSV file to CartoDB dashboard

### Convert Darwin Core Archive to Darwin Core CSV

The `dwca2csv.py` Python script downloads, unzips, and converts a [Darwin Core Archive](http://www.gbif.org/informatics/standards-and-tools/publishing-data/data-standards/darwin-core-archives) from an [IPT](http://www.gbif.org/informatics/infrastructure/publishing/) installation into a Darwin Core CSV file that can be uploaded to CartoDB. It requires Python version 2.7 or greater.

Example usage:

```bash
./dca2csv.py -u http://vertnet.nhm.ku.edu:8080/ipt/archive.do?r=nysm_mammals
```

That will create and change into a directory called `nysm_mammals`, download and expand the Darwin Core Archive into a file named `nysm_mammals.zip`. It will expand the archive into `eml.xml`, `meta.xml`, and `occurrence.txt`. Finally, it will use these files to create `nysm_mammals.csv` which can be uploaded to CartoDB.

### Upload CSV file to CartoDB dashboard

Access the CartoDB dashboard and then simply drag and drop the CSV file into the browser to upload it. More details are in the [CartoDB documentation](http://developers.cartodb.com/documentation/using-cartodb.html#managing_tables).
