# GSoC 2017 Report

**Name:** Rishi Shah

**Email id:** rishiloyola98245@gmail.com

## First Evaluation

### In terms of pull requests:

**Contributions Before GSoC:**
- Add support to disable the auth through command line flag. [link](https://github.com/vkuznet/transfer2go/pull/2)
- Did code refactoring of the existing code. [link](https://github.com/vkuznet/transfer2go/pull/4)

**Contributions During Bonding Period:**
- Added unit/integration test suite [link](https://github.com/vkuznet/transfer2go/pull/20)
- Add logrus logger [link](https://github.com/vkuznet/transfer2go/pull/22)

**Contributions During Coding Period:**
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


## Final Evaluation

### In terms of pull requests:

- Add smart router [link](https://github.com/vkuznet/transfer2go/pull/42)
- Run transfer2go in separate process [link](https://github.com/vkuznet/transfer2go/pull/36)
- Do chunk wise transfer [link](https://github.com/vkuznet/transfer2go/pull/37)
- Change and to or condition [link](https://github.com/vkuznet/transfer2go/pull/38)
- Improve unique id generator [link](https://github.com/vkuznet/transfer2go/pull/39)
- Add client html page [link](https://github.com/vkuznet/transfer2go/pull/40)
- Avoid duplicate transfer [link](https://github.com/vkuznet/transfer2go/pull/41)
- Close file in go routine [link](https://github.com/vkuznet/transfer2go/pull/43)

### In brief:

`July`: Setup the test environment. Performed the test between using uibo-cms-02.cr.cnaf.infn.it. uibo-cms-02.cr.cnaf.infn.it is one of the grid UIs located at the INFN-CNAF Tier-1 centre of WLCG, in Bologna (Italy). Wrote the test report. ([link](https://docs.google.com/document/d/1UUXQIuef8_UTwpABDfjiW934P9RrVEWJMcknIKqRZ8A/edit?usp=sharing))

`August`: Resolved couple of bugs in existing system. Wrote proposal for the ML router. ([link](https://docs.google.com/document/d/1H-LVskFrkLrbMhEi9zNuJpKLDa7Dli7P4aR6gyBHjhQ/edit?usp=sharing)). Implementing smart-router.
