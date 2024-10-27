.. _topics-changelog:

====
Changelog
====

0.12.0
======
Friday, October 04th, 2024
--------------------------
- Store summary of previous runs when deflaking
- Runner Client Minor Refactor and Test
- Adding nodes used in test summary for HTML report
- Parse args from config files independently
- add support to python 3.10, 3.11 and 3.12


0.11.4
======
Friday, August 18th, 2023
-------------------------
- Updated `requests` version to 2.31.0

0.11.3
======
Wednesday, November 30th, 2022
------------------------------
- Bugfix: fixed an edge case when BackgroundThread wait() method errors out if start() method has never been called.

0.11.2
======
- automatic release process bugged out; ignore :)

0.11.1
======
- Removed `tox` from requirements. It was not used, but was breaking our builds due to recent pushes to `virtualenv`.
- Bumped `jinja2` to `3.0.x`

0.11.0
======
- Option to fail tests without `@cluster` annotation. Deprecate ``min_cluster_spec()`` method in the ``Test`` class - `#336 <https://github.com/confluentinc/ducktape/pull/336>`_

0.10.3
======
Friday, August 18th, 2023
-------------------------
- Updated `requests` version to 2.31.0

0.10.2
======
- Removed `tox` from requirements. It was not used, but was breaking our builds due to recent pushes to `virtualenv`.

0.10.1
======
- Disable health checks for nodes, effectively disabling `#325 <https://github.com/confluentinc/ducktape/pull/325>`_. See github issue for details - `#339 <https://github.com/confluentinc/ducktape/issues/339>`_

0.10.0
======
- **DO NOT USE**, this release has a nasty bug - `#339 <https://github.com/confluentinc/ducktape/issues/339>`_
- Do not schedule tests on unresponsive nodes - `#325 <https://github.com/confluentinc/ducktape/pull/325>`_

0.9.4
=====
Friday, August 18th, 2023
-------------------------
- Updated `requests` version to 2.31.0

0.9.3
=====
- Removed `tox` from requirements. It was not used, but was breaking our builds due to recent pushes to `virtualenv`.

0.9.2
=====
- Service release, no ducktape changes, simply fixed readthedocs configs.

0.9.1
=====
- use a generic network device based on the devices found on the remote machine rather than a hardcoded one - `#314 <https://github.com/confluentinc/ducktape/pull/314>`_ and `#328 <https://github.com/confluentinc/ducktape/pull/328>`_
- clean up process properly after an exception during test runner execution - `#323 <https://github.com/confluentinc/ducktape/pull/323>`_
- log ssh errors - `#319 <https://github.com/confluentinc/ducktape/pull/319>`_
- update vagrant tests to use ubuntu20 - `#328 <https://github.com/confluentinc/ducktape/pull/328>`_
- added command to print the total number of nodes the tests run will require - `#320 <https://github.com/confluentinc/ducktape/pull/320>`_
- drop support for python 3.6 and add support for python 3.9 - `#317 <https://github.com/confluentinc/ducktape/pull/317>`_

0.9.0
=====
- Upgrade paramiko version to 2.10.0 - `#312 <https://github.com/confluentinc/ducktape/pull/312>`_
- Support SSH timeout - `#311 <https://github.com/confluentinc/ducktape/pull/311>`_

0.8.18
======
- Updated `requests` version to `2.31.0`

0.8.17
======
- Removed `tox` from requirements. It was not used, but was breaking our builds due to recent pushes to `virtualenv`.

0.8.x
=====
- Support test suites
- Easier way to rerun failed tests - generate test suite with all the failed tests and also print them in the log so that user can copy them and paste as ducktape command line arguments
- Python 2 is no longer supported, minimum supported version is 3.6
- Added `--deflake N` flag - if provided, it will attempt to rerun each failed test  up to N times, and if it eventually passes, it will be marked as Flaky - `#299 <https://github.com/confluentinc/ducktape/pull/299>`_
- [backport, also in 0.9.1] - use a generic network device based on the devices found on the remote machine rather than a hardcoded one - `#314 <https://github.com/confluentinc/ducktape/pull/314>`_ and `#328 <https://github.com/confluentinc/ducktape/pull/328>`_
- [backport, also in 0.9.1] - clean up process properly after an exception during test runner execution - `#323 <https://github.com/confluentinc/ducktape/pull/323>`_
- [backport, also in 0.9.1] - log ssh errors - `#319 <https://github.com/confluentinc/ducktape/pull/319>`_
- [backport, also in 0.9.1] - update vagrant tests to use ubuntu20 - `#328 <https://github.com/confluentinc/ducktape/pull/328>`_
- [backport, also in 0.9.1] - added command to print the total number of nodes the tests run will require - `#320 <https://github.com/confluentinc/ducktape/pull/320>`_
