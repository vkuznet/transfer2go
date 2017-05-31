CREATE TABLE FILES(id INTEGER PRIMARY KEY, lfn TEXT UNIQUE, pfn TEXT, blockid INTEGER, datasetid INTEGER, bytes INTEGER, hash TEXT, transfertime INTEGER, timestamp INTEGER, FOREIGN KEY(blockid) REFERENCES BLOCKS(id), FOREIGN KEY(datasetid) REFERENCES DATASETS(id));
CREATE TABLE DATASETS(id INTEGER PRIMARY KEY, dataset TEXT UNIQUE);
CREATE TABLE BLOCKS(id INTEGER PRIMARY KEY, block TEXT UNIQUE, datasetid INTEGER, FOREIGN KEY(datasetid) REFERENCES DATASETS(id));
