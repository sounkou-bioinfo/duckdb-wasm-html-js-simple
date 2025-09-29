server <- function(static_dir = system.file("static", package = "duckdbWasmHtmlJsSimple"),
                   goserver_addr = "0.0.0.0:8081") {
    current_dir <- getwd() |>
        normalizePath(mustWork = TRUE)
    print(current_dir)
    csv_path <- file.path(current_dir, "mtcars.csv")
    # Create mtcars.csv at startup if not exists (base R)
    if (!file.exists(csv_path)) {
        write.csv(mtcars, csv_path, row.names = FALSE)
    }

    # Launch goserveR using a launcher script and get its PID
    pid_file <- tempfile("goserver_pid_")
    system2(
        command = R.home("bin/Rscript"),
        args = c(
            file.path(getwd(), "goserver_launcher.R"),
            pid_file,
            goserver_addr,
            current_dir
        ),
        wait = FALSE
    )
    Sys.sleep(2)
    if (!file.exists(pid_file)) {
        stop("Failed to launch goserveR: PID file not created.")
    }
    goserver_pid <- as.integer(readLines(pid_file, warn = FALSE))
    if (is.na(goserver_pid) || goserver_pid <= 0) {
        stop("Failed to launch goserveR: Invalid PID.")
    }

    # Define plumber2 API
    pr <- api()

    # Serve static assets at /assets
    pr <- api_statics(pr, "/assets", static_dir)

    # Endpoint to list files in current directory and return HTTP address
    pr <- api_get(pr, "/list-files", function(req, res) {
        files <- list.files(current_dir, full.names = TRUE) |>
            normalizePath(mustWork = TRUE)
        file_urls <- lapply(files, function(f) {
            list(
                name = basename(f),
                url = sprintf("http://%s%s", goserver_addr, f)
            )
        })
        list(files = file_urls)
    }, serializers = get_serializers("json"))

    # Example endpoint for DuckDB WASM testing
    pr <- api_get(pr, "/duckdb-test", function(req, res) {
        http_loaded <- !is.null(req$HTTP_REFERER) && grepl("http", req$HTTP_REFERER)
        list(
            message = "DuckDB WASM test endpoint",
            http_loaded = http_loaded,
            csv_url = sprintf("http://%s/mtcars.csv", goserver_addr)
        )
    }, serializers = get_serializers("json"))

    # Kill Go server on plumber2 exit
    pr <- api_on(pr, "end", function() {
        if (!is.na(goserver_pid) && goserver_pid > 0) {
            system(sprintf("kill -9 %d", goserver_pid))
        }
    })

    api_run(pr)
}
