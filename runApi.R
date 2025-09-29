#!/usr/bin/env Rscript

# Load the package and run the API
devtools::load_all()
library(duckdbWasmHtmlJsSimple)
library(plumber2)
library(goserveR)
server()
