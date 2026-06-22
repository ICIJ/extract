<h3 align="center">Extract</h3>

<div align="center">
<p>A cross-platform command line tool for parallelized, distributed content extraction.</p>

| | Status |
| --: | :-- |
| **CI checks** | [![CircleCI](https://img.shields.io/circleci/build/gh/ICIJ/extract.svg?style=flat)](https://circleci.com/gh/ICIJ/extract) |
| **Maven Central** | [![Maven Central](https://img.shields.io/maven-central/v/org.icij.extract/extract-lib?style=flat)](https://central.sonatype.com/artifact/org.icij.extract/extract-lib) |
| **Latest version** | [![Latest version](https://img.shields.io/github/v/tag/icij/extract?style=flat)](https://github.com/ICIJ/extract/releases/latest) |
| **Open issues** | [![Open issues](https://img.shields.io/github/issues/icij/extract?style=flat&color=success)](https://github.com/ICIJ/extract/issues/) |
| **Documentation** | [![Wiki](https://img.shields.io/badge/Wiki-193D87?style=flat)](https://github.com/ICIJ/extract/wiki) |

</div>

# Extract

**Extract** is an open‑source, cross‑platform command line tool for parallelized, distributed content extraction. Built on top of [Apache Tika](https://tika.apache.org/), it parses text and metadata from heterogeneous files (PDFs, emails, office documents, images, archives, etc.) and can run optical character recognition (OCR) on scans. It uses Redis‑backed queueing to distribute work across many machines and writes results to Solr, plain text files, or standard output. Extract is an essential part of the engineering behind the [Panama Papers](https://en.wikipedia.org/wiki/Panama_Papers), [Swiss Leaks](https://en.wikipedia.org/wiki/Swiss_Leaks) and [Luxembourg Leaks](https://en.wikipedia.org/wiki/Luxembourg_Leaks) investigations.

<!-- omit from toc -->
## Table of Contents

- [Main Features](#main-features)
- [Developer Guide](#developer-guide)
  - [Requirements](#requirements)
  - [Build](#build)
  - [Run Tests](#run-tests)
  - [Release](#release)
- [License](#license)
- [About ICIJ](#about-icij)

## Main Features

* 📄 **Tika‑powered extraction**: Parse text and metadata from PDFs, emails, office docs, images, archives, and more.
* 🖼️ **OCR on scans & images**: Turn visual text into machine‑readable text via Tesseract.
* ⚡ **Parallel extraction**: Process many files concurrently on a single machine.
* 🌐 **Distributed queueing**: Coordinate extraction across multiple machines with a Redis‑backed queue.
* 📤 **Pluggable output**: Write extracted content to Solr, plain text files, or standard output.

## Developer Guide

This section explains how to set up a development environment, build the project, and run the tests. It assumes you are comfortable with Java/Maven projects. The project is modular and split into two Maven modules: `extract-lib` (the extraction library, published to Maven Central) and `extract-cli` (the command line interface).

### Requirements

* **JDK 17**
* **Apache Maven 3.8+** - primary build tool
* **GNU Make** (optional) - convenient shortcuts (run `make help` to see available targets)
* **Redis 4+** - distributed extraction queue; also required to run the test suite
* **Tesseract OCR & Leptonica** (optional) - required for OCR of scans and images

A local Redis instance is enough to run the tests:

```bash
docker run -d -p 6379:6379 redis:alpine
```

### Build

The project is modular. Using Make:

```bash
# Build and install all modules to the local Maven repository
make install

# Or build the distribution JARs only (skips tests)
make build
```

### Run Tests

Extract has both unit and integration tests. Integration tests expect a Redis instance to be reachable, and OCR tests expect Tesseract and Leptonica to be installed.

```bash
# Run the whole test suite
make test

# Or run a single module
mvn -pl extract-lib test

# Or a single test class
mvn -pl extract-lib -Dtest=org.icij.extract.ExtractorTest test
```

### Release

`extract-lib` is published to Maven Central. To cut a new release:

```bash
make release NEW_VERSION=x.y.z
```

This sets the new version across the modules, commits the change, and tags it. Push the tag to trigger the CircleCI release job:

```bash
git push origin master --tags
```

For practical command‑line usage of the extraction tool itself, see the [wiki](https://github.com/ICIJ/extract/wiki).

## License

Extract is distributed under the [MIT License](LICENSE).

## About ICIJ

The **International Consortium of Investigative Journalists (ICIJ)** is a global network of reporters and media organizations collaborating on cross‑border investigations (e.g., *Panama Papers*, *Luanda Leaks*, *Uber Files*, *Pandora Papers*). The tech team at ICIJ builds tools like Extract to empower investigative journalism at scale, handling millions of documents securely and efficiently.

**Contact & Community**

* Issues & feature requests: [GitHub Issues](https://github.com/ICIJ/extract/issues)
* Email: `engineering@icij.org`
* Security reports: please email us and avoid filing public issues for vulnerabilities.
