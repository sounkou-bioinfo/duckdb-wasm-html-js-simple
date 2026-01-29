#!/usr/bin/env Rscript

library(duckdbWasmHtmlJsSimple)
library(goserveR)
library(plumber2)

static_dir <- file.path(getwd(), "inst/static")
go_server_prefix <- "/static"
go_server_port <- 8000
plumber2port <- 8080
plumber2static_prefix <- "/static"
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
api <- plumber2::api()
api <- api |>
    plumber2::api_forward(plumber2static_prefix, paste0("http://", go_addr, go_server_prefix))

api |> plumber2::api_run(port = plumber2port)
