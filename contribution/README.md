# GSoC 2017 Report

**Name:** Rishi Shah

**Email id:** rishiloyola98245@gmail.com

## First Evaluation

### In terms of pull requests:

**Contributions Before GSoC :**
- Add support to disable the auth through command line flag. [link](https://github.com/vkuznet/transfer2go/pull/2)
- Did code refactoring of the existing code. [link](https://github.com/vkuznet/transfer2go/pull/4)

**Contributions During Bonding Period :**
- Added unit/integration test suite [link](https://github.com/vkuznet/transfer2go/pull/20)
- Add logrus logger [link](https://github.com/vkuznet/transfer2go/pull/22)

**Contributions During Coding Period :**
- Benchmark the system [link](https://github.com/vkuznet/transfer2go/pull/23)
- Switch to pull based model [link](https://github.com/vkuznet/transfer2go/pull/26)

### In brief during Coding period:

`Week1:` Did the benchmark of sqlite db. Wrote a script to generate thousands of records and to benchmark the queries. Made small changes in existing database schema.

`Week2:` Added data structure to sort and store the requests in buffer. Added new db to store the records of transfer requests. Currently, designing new architecture for the pull based model.

## Second Evaluation

### In terms of pull requests:

- Implemented Pull model [link](https://github.com/vkuznet/transfer2go/pull/26)
- Added supervisor to run the process 24 by 7 [link](https://github.com/vkuznet/transfer2go/pull/30)
- Designed new web interface [link](https://github.com/vkuznet/transfer2go/pull/32)
- Resolved deadlock condition [link](https://github.com/vkuznet/transfer2go/pull/33)
- Added deployment script [link](https://github.com/vkuznet/transfer2go/pull/34)

### In brief:

`May:` During the may end I have designed the new architecture for the pull model.

`June`: Implemented pull based model in such a way that we can switch to push or pull model.

`July`: Did testing and code refactoring. Currently, testing pull based model using CERN machines.
