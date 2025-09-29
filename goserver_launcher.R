args <- commandArgs(trailingOnly = TRUE)
pid_file <- args[1]
addr <- args[2]
dir <- args[3]
write(Sys.getpid(), pid_file)
goserveR::runServer(addr = addr, dir = dir)
