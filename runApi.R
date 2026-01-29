#!/usr/bin/env Rscript

library(duckdbWasmHtmlJsSimple)
library(goserveR)
library(ambiorix)

static_dir <- file.path(getwd(), "inst/static")
go_server_prefix <- "/static"
go_server_port <- 8000
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
go_addr <- paste0("127.0.0.1:", go_server_port)
go_server <- goserveR::runServer(
    addr = go_addr, dir = static_dir, prefix = go_server_prefix,
    blocking = FALSE
)
Sys.sleep(5)
get_static <- function(req, res) {
    res$status <- 304L
    # redirect to go server static files
    print(req)
    res$redirect(path = paste0("http://", go_addr, req$PATH_INFO))
}
get_static_file <- function(req, res) {
    # param
    file <- req$params$filepath
    res$status <- 304L
    # redirect to go server static files
    message("looking for file: ", file)
    res$redirect(path = paste0("http://", go_addr, go_server_prefix, "/", file))
}

app <- Ambiorix$new()
router <- Router$new("/static")
app$get("/static", get_static)
app$get("/static/:filepath", get_static_file)
app$get("/", function(req, res) {
    res$send("Home!")
})
app$start(port = ambiorix_port)
