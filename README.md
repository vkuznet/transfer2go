# transfer2go

[![Build Status](https://travis-ci.org/vkuznet/transfer2go.svg?branch=master)](https://travis-ci.org/vkuznet/transfer2go)
[![Go Report Card](https://goreportcard.com/badge/github.com/vkuznet/transfer2go)](https://goreportcard.com/report/github.com/vkuznet/transfer2go)
[![GoDoc](https://godoc.org/github.com/vkuznet/transfer2go?status.svg)](https://godoc.org/github.com/vkuznet/transfer2go)
[![DOI](https://zenodo.org/badge/80159126.svg)](https://zenodo.org/badge/latestdoi/80159126)

Go implementation of CMS
[PhEDEx](https://www.researchgate.net/publication/228732867_Data_transfer_infrastructure_for_CMS_data_taking)
distributed, loosly coupled agents for CMS transfering data.

### Description
The [CMS](http://cms.web.cern.ch/) experiment at the LHC proton-proton collider
developed PhEDEx (Physics Experiment Data Export) service as reliable and
scalable data management system to meet experiment requirements in Run I/II.
Designed in 2004, and written mainly in Perl, it is still used today for
managing multi-PB transfer loads per week, across a complex topology of dozen
of Grid-compliant computing centres.

Its architecture, instead of having a central brain making global decisions on
all CMS replica allocation, has a data management layer composed of a set of
loosely coupled and stateless software agents - each managing highly specific
parts of replication operations at each site in the distribution network -
which communicate asynchronously through a central blackboard architecture.
The system is resilient and robust against a large variety of possible failure
reasons, and it has been designed by assuming a transfer will fail (thus
implementing fail-over tactics) and being completely agnostic on the
lower-level file transfer mechanism (thus focusing on full dataset management
and detailed transfer bookkeeping). Collision data and derived data collected
at LHC that allowed to achieve the Higgs boson discovery by ATLAS and CMS
experiments at LHC were transferred in the CMS worldwide domain using this
toolkit.

The aim of this project is to extend basic PhEDEX functionality
to address up-coming challenges in exa-byte HL-HLC era via implementation of modern
Go programming language.

The motivation for the effort is many fold:
- eliminate central blackboard system and necessity to rely on ORACLE backend
  via fully distributed nature of the agents, self-discovery and task
  delegation;
- even though current system is working well it lacks of support and expertise
  in perl programming language. We would like to leverage modern language such as
  Go to enhance concurrency model via native support in a language, and
  dependency free deployment;
- the data volume expected in HL-HLC era will grow significantly to exa-byte
  level and we need to explore elasticity approach to handle variety of
  opportunistic resources;
- extend file access and transfer patterns to event streaming, or individual objects, etc.;
- implement support for user produced data in addition to centrally produced and manager by the system
- take advantage of built-in concurrency model of the Go language and explore the scalability boundaries.
