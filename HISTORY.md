# CIHM::WIP module history

## Short version

The git log suggests this code was created on November 22, 2017, and that it was created by Russell McOrmond.

This was the date that code was refactored while it was in our Subversion repository, and included many 'svn mv' commands from across our repository to land in CIHM-WIP/trunk .

When we tried to use `git svn` to move to Git to publish on Github, we lost the history prior to moving into the new project. We tried many different ways to extract the history, including a perl script to filter the output of `svnadmin dump` , but the problem turned out to be too messy.  We decided to leave that history in the Subversion repository, and create this note to reference the issue.

As of January 12, 2018 when the internal http://svn.c7a.ca/svn/c7a/ repository is at revision 6786.

This repository was created using:

`git svn clone file:///data/svn/c7a -T CIHM-WIP/trunk --authors-file=/home/git/authors.txt --no-metadata -s CIHM-WIP`

## Longer version

Design work for what we called 'ingest automation' started in April 2015, implimentation wasn't prioritised until the fall of 2016.

For this specific PERL module all the work was done by Russell McOrmond.

CIHM::WIP::Ingest::Worker impliments processes previously done through the `tdr ingest` command-line tools, but refactored to work as a microservice where parameters are sourced from CouchDB.

CIHM::WIP::Mallet::Worker impliments processes previously done through a wide variety of custom SIP building tools that were created per-project. Bringing all of this custom scripting into a single simplified workflow was the focus of the design work.

