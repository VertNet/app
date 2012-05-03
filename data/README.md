# Overview

This directory contains tools for working with data.

# Darwin Core Archives → CartoDB

Right now the process of converting a [Darwin Core Archive](http://www.gbif.org/informatics/standards-and-tools/publishing-data/data-standards/darwin-core-archives) into a Darwin Core CSV file is a leetle painful. This will be automated down the road,  but for now here are the steps:
1.  Convert Darwin Core Archive to Darwin Core CSV file.
2.  Upload CSV file to CartoDB dashboard

### Convert Darwin Core Archive to Darwin Core CSV

The `dwca2csv.py` Python script downloads, unzips, and converts a [Darwin Core Archive](http://www.gbif.org/informatics/standards-and-tools/publishing-data/data-standards/darwin-core-archives) into a Darwin Core CSV file that can be uploaded to CartoDB.

Example usage:

```bash
./dca2csv.py -d data.csv -u http://vertnet.nhm.ku.edu:8080/ipt/archive.do?r=nysm_mammals
```

That will download and expand the Darwin Core Archive into a file named `dwca.zip`. It will expand the archive into `eml.xml`, `meta.xml`, and `occurrence.txt`. Finally, it will use these files to create `data.csv` which can be uploaded to CartoDB.

### Upload CSV file to CartoDB dashboard

Access the CartoDB dashboard and then simply drag and drop the CSV file into the browser to upload it. More details are in the [CartoDB documentation](http://developers.cartodb.com/documentation/using-cartodb.html#managing_tables).
