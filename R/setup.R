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


#' Takes the result of the processed files from above
#' and combines them into one dataframe, per ad_id/exchange
#'
#' @param ad_id the ad_id to operate on
#' @param exchange the exchange to use
#' @param input_folder where the data is stored (output_folder from read_split_save)
#' @return a data frame, the result of joining the four and calling clean_joined_data
#' @author Colman Humphrey
#'
#' @export
create_combined_data <- function(ad_id,
                                 exchange,
                                 input_folder){
    if (!(ad_id %in% c("1458", "3358", "3386", "3427", "3476"))) {
        warning("not sure if ad_id ", ad_id, " known", call. = FALSE)
    }

    if (!(exchange %in% c("1", "2", "3"))) {
        warning("not sure if exchange ", exchange, " known", call. = FALSE)
    }

    message("combining data for ad_id ", ad_id, ", exchange ", exchange)

    ##----------------

    input_ad_folder <- paste0(input_folder, "/", ad_id, "/")

    bid_data <- data.table::fread(paste0(input_ad_folder,
                                         "bid_exchange", exchange, ".csv"),
                                  integer64 = "character")

    ## adding the others
    use_cols <- c("ipinyou_id", "log_type", "paying_price", "landing_page_url")
    for (data_type in c("clk", "conv", "imp")) {
        input_file <- paste0(input_ad_folder, data_type,
                             "_exchange", exchange, ".csv")
        if (!file.exists(input_file)) {
            message("    ", data_type, ": file not found (`",
                    input_file, "`)")
            for (use_col in use_cols) {
                bid_data[[paste0(data_type, "_", use_col)]] <- NA
            }
        } else {
            message("    ", data_type, ": joining")
            dt_frame <- data.table::fread(input_file,
                                          integer64 = "character",
                                          select = c("bid_id", use_cols, "timestamp"))
            names(dt_frame) <- c("bid_id", paste0(data_type, "_", use_cols), "timestamp")

            ## solve duplicates by assuming it's the min timestamp
            dt_frame <- dt_frame[order(timestamp)][!duplicated(bid_id)][, timestamp:=NULL]

            bid_data <- merge(bid_data, dt_frame,
                              by = c("bid_id"), all.x = TRUE)
        }
    }

    message("cleaning joined data")
    ## ok data.table sucks
    clean_joined_data(dplyr::as_tibble(bid_data))
}


#' Takes first non-missing value in a series of inputs
#'
#' @param ... something like a bunch of vectors
#' @return an element same length as the original, but with the first non-missing
#' for each entry, if any (else just missing)
#' @author Colman Humphrey
coalesce <- function(...){
    rel_list <- list(...)
    if (length(unique(lengths(rel_list))) > 1L) {
        stop("all inputs must have the same length to coalesce",
             call. = FALSE)
    }

    pair_coalesce <- function(x, y){
        x[is.na(x)] <- y[is.na(x)]
        x
    }

    Reduce(pair_coalesce, rel_list)
}

#' Given a set of values of some kind, creates sawtooth summation of them
#'
#' @param input_vec
#' @return vector same length as input_vec, but integers
#' @author Colman Humphrey
sawtooth <- function(input_vec){
    lgl_vec <- c(FALSE,
                 input_vec[2:length(input_vec)] ==
                 input_vec[1:(length(input_vec) - 1)])
    lgl_vec[is.na(input_vec)] <- FALSE

    falses <- c(which(!lgl_vec), length(lgl_vec) + 1L)
    unlist(lapply(falses[2:length(falses)] - falses[1:(length(falses) - 1)],
                  function(len){
                      1:len
                  }))
}


#' Does further parsing of the joined data
#'
#' @param full_data joined data produced in create_combined_data
#' @return dataframe with same rows as full_data, but with some new columns
#' (and without some old ones)
#' @author Colman Humphrey
clean_joined_data <- function(full_data){
    message("removing literal \"null\" strings")
    for (col_name in names(full_data)) {
        null_index <- full_data[[col_name]] == "null"
        full_data[[col_name]][null_index] <- NA
    }

    full_data[["paying_price"]] <- full_data[["imp_paying_price"]]
    full_data$clk_paying_price <- NULL
    full_data$imp_paying_price <- NULL
    full_data$conv_paying_price <- NULL

    ## we'll keep log types, they're not the same
    full_data[["ipinyou_id"]] <- coalesce(full_data[["ipinyou_id"]],
                                          full_data[["imp_ipinyou_id"]],
                                          full_data[["clk_ipinyou_id"]],
                                          full_data[["conv_ipinyou_id"]])
    full_data[["landing_page_url"]] <- coalesce(
        full_data[["imp_landing_page_url"]],
        full_data[["clk_landing_page_url"]],
        full_data[["conv_landing_page_url"]])
    full_data$clk_landing_page_url <- NULL
    full_data$imp_landing_page_url <- NULL
    full_data$conv_landing_page_url <- NULL

    ##----------------

    message("parsing user agent (this is slow...)")
    ua_strings <- full_data$user_agent
    ua_uni <- unique(ua_strings)

    ua_parsed_frame = as.data.frame(uaparserjs::ua_parse(ua_uni, .progress = TRUE))
    ua_full_frame = ua_parsed_frame[
        match(ua_strings, ua_uni),
        c("ua.family", "os.family", "device.family")]
    message("user agent parsed")

    full_data <- cbind(full_data, ua_full_frame)

    ## tried frank to solve this, way too slow

    full_data <- full_data[order(full_data$ipinyou_id,
                                 full_data$timestamp), ]
    full_data$user_bid_num <- sawtooth(full_data$ipinyou_id)

    full_data
}
