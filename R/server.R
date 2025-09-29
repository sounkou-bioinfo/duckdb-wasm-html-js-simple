#' Serve static files for DuckDB-Wasm web apps using Go HTTP server
#'
#' @param static_dir Directory to serve (default: inst/static in current working directory)
#' @param goserver_addr Address to bind the server (default: "127.0.0.1:8081")
#' @param prefix URL prefix for served files (default: "/static")
#' @return None. Blocking call. Cleans up mtcars.csv on exit.
#' @export
server <- function(static_dir = file.path(getwd(), "inst/static"),
                   goserver_addr = "127.0.0.1:8081",
                   prefix = "/static") {
    if (!dir.exists(static_dir)) dir.create(static_dir, recursive = TRUE)
    csv_path <- file.path(static_dir, "mtcars.csv")
    write.csv(mtcars, csv_path, row.names = FALSE)
    on.exit(
        {
            if (file.exists(csv_path)) file.remove(csv_path)
        },
        add = TRUE
    )
    goserveR::runServer(addr = goserver_addr, dir = static_dir, prefix = prefix)
}
