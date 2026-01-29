#!/usr/bin/env Rscript

# Load the package and run the API
devtools::load_all()
library(duckdbWasmHtmlJsSimple)
library(goserveR)
library(ambiorix)

static_dir <- file.path(getwd(), "inst/static")
go_server_prefix <- "/static"
go_server_port <- 8081
ambiorix_port <- 8080
ambiorix_static_prefix <- "/static"
csv_path <- file.path(static_dir, "mtcars.csv")
message("Writing mtcars.csv to ", csv_path)
write.csv(mtcars, csv_path, row.names = FALSE)
pq_path <- file.path(static_dir, "mtcars.parquet")
message("Writing mtcars.parquet to ", pq_path)
nanoparquet::write_parquet(mtcars, pq_path)
on.exit(
    {
        if (file.exists(csv_path)) file.remove(csv_path)
        if (file.exists(pq_path)) file.remove(pq_path)
    },
    add = TRUE
)

goserveR::runServer(
    addr = paste0("127.0.0.1:", go_server_port), dir = static_dir, prefix = go_server_prefix,
    blocking = FALSE
)
Sys.sleep(5)
get_static <- function(req, res) {
    res$status <- 302L
    # redirect to go server static files
    res$redirect(path = paste0("http://127.0.0.1:", go_server_port, , "/static"))
}

app <- Ambiorix$new()

app$get("/static", get_static)

app$start(port = ambiorix_port)
