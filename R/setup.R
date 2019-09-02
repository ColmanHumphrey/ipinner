#' Reads in ipinyou data and saves in files split by ad_id,
#' data type and exchange
#'
#' The ipinyou data comes already split by data type, but not by ad_id
#' or exchange, and it has the dates separated. This function is to rearrange this
#' process by combining dates (unsplitting raw data) but splitting on ad_id and
#' exchange.
#' 
#' @param data_type one of bid, clk, conv, imp
#' @param data_date date in the form YYYYMMDD
#' @param col_names names of the columns; only bid should be different (smaller)
#' @param input_folder the folder where the input data lives; probably something like
#' "../../data/ipinyou.contest.dataset/training2nd/"
#' @param output_folder where the processed files should go, something like
#' "../../data/training2nd_adsplit"
#' @return TRUE if file existed and we split just fine, else FALSE
#' @author Colman Humphrey
#'
#' @export
read_split_save <- function(data_type,
                            data_date,
                            col_names,
                            input_folder,
                            output_folder){
    if (!(data_type %in% c("bid", "clk", "conv", "imp"))) {
        stop("data_type ", data_type, " unknown", call. = FALSE)
    }

    if (is.na(as.Date(data_date, format = "%Y%m%d"))) {
        stop("data_date ", data_date, "not recognised as format YYYYMMDD",
             call. = FALSE)
    }
    
    dest_file = paste0(input_folder, data_type, '.', data_date, '.txt')

    if (!file.exists(dest_file)) {
        message(paste0("can't find file ", dest_file, "\n"))
        return(FALSE)
    }

    tryCatch({
        message(paste0("reading ", dest_file))
        temp_table <- data.table::fread(input = dest_file,
                                        sep = "\t",
                                        integer64 = "character")
        message(paste0("    dim: (",
                       paste0(dim(temp_table), collapse = ", "),
                       ")"))

        colnames(temp_table) <- col_names

        ad_list <- base::split(temp_table, f = temp_table[["advertiser_id"]])
        rm(temp_table)

        lapply(ad_list, function(ad_table){
            ad_id <- ad_table[["advertiser_id"]][1]
            message(paste0("    processing ad_id ", ad_id))

            ad_id_dir <- paste0(output_folder, "/", ad_id)

            if (!dir.exists(ad_id_dir)) {
                dir.create(ad_id_dir)
            }

            exchange_split <- split(ad_table, f = ad_table[["ad_exchange"]])

            for(ex in names(exchange_split)){
                message(paste0("    writing exchange ", ex))

                write_file <- paste0(ad_id_dir, "/", data_type, "_exchange",
                                     ex, ".csv")
                
                data.table::fwrite(x = exchange_split[[ex]],
                                   file = write_file,
                                   append = file.exists(write_file))
            }
        })
        return(TRUE)
    }, error = function(e){
        message("process failed with message ", e$message)
        return(FALSE)
    })
}
