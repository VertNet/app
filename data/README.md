# Overview

This directory contains tools for working with data.

# Darwin Core Archives to CSV

The `dwca2csv.py` Python script downloads, unzips, and converts a [Darwin Core Archive](http://www.gbif.org/informatics/standards-and-tools/publishing-data/data-standards/darwin-core-archives) into a Darwin Core CSV file that can be uploaded to CartoDB.

Example usage:

```bash
./dca2csv.py -d data.csv -u http://vertnet.nhm.ku.edu:8080/ipt/archive.do?r=nysm_mammals
```

That will download and expand the Darwin Core Archive into a file named `dwca.zip`. It will expand the archive into `eml.xml`, `meta.xml`, and `occurrence.txt`. Finally, it will use these files to create `data.csv` which can be uploaded to CartoDB.
