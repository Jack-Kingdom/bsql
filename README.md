# bsql
[![Go Report Card](https://goreportcard.com/badge/github.com/Jack-Kingdom/bsql)](https://goreportcard.com/report/github.com/Jack-Kingdom/bsql)
[![GoDoc](https://godoc.org/github.com/Jack-Kingdom/bsql?status.svg)](https://godoc.org/github.com/Jack-Kingdom/bsql)
[![Build Status](https://travis-ci.org/Jack-Kingdom/bsql.svg?branch=master)](https://travis-ci.org/Jack-Kingdom/bsql)
[![codecov](https://codecov.io/gh/Jack-Kingdom/bsql/branch/master/graph/badge.svg)](https://codecov.io/gh/Jack-Kingdom/bsql)
[![License](https://img.shields.io/badge/License-MIT%20-blue.svg)](https://github.com/Jack-Kingdom/bsql/blob/main/LICENSE)

bsql wrap golang std sql package, make it more easy to use.

## Features
- Easy to bind query result into struct
- Builtin prepared statement for better performance & against SQL injection
- Unify err type, no need check err for different kind of database
