##------------------------------------
## Here, we'll focus on the "2nd", which we discovered was the cleanest dataset

## The files are pretty big so we (probably) can't just read them all in

## First part:
## 1. read in the data, by data type and date
## 2. split these by ad_id, exchange, (continue to split on data type), but
##    COMBINE dates by appending
## 3. will create folders of the form "training2nd_adsplit/<ad_id>"
## 4. files in these folders will be "<data_type>_exchange{1, 2, 3}.csv"

## Second part:
## 1. for each ad_id, exchange, read in bid
## 2. join the other three
## 3. clean up and save the result

##------------------------------------

## Make sure:
## devtools::install_github("ColmanHumphrey/ipinner")

##------------------------------------

data_folder <- "data/"

input_folder <- paste0(data_folder, "ipinyou.contest.dataset/training2nd/")
data_dates <- paste0("201306",
                     formatC(6:12, width = 2, flag = "0"))
data_types <- c("bid", "clk", "conv", "imp")

## change this for a different folder if you want:
adsplit_folder <- paste0(data_folder, "training2nd_adsplit")
if (dir.exists(adsplit_folder)) {
    dir.create(adsplit_folder)
}

##------------------------------------

all_colnames = c("bid_id",
                 "timestamp",
                 "log_type", #
                 "ipinyou_id",
                 "user_agent",
                 "ip",
                 "region_id",
                 "city_id",
                 "ad_exchange",
                 "domain",
                 "url",
                 "anon_url",
                 "ad_slot_id",
                 "ad_slot_width",
                 "ad_slot_height",
                 "ad_slot_visibility",
                 "ad_slot_format",
                 "ad_slot_floor_price",
                 "creative_id",
                 "bidding_price",
                 "paying_price", #
                 "landing_page_url", #
                 "advertiser_id",
                 "user_profile_id")

not_bid <- c("log_type", "paying_price", "landing_page_url")
bid_colnames <- all_colnames[!(all_colnames %in% not_bid)]

##------------------------------------

for (data_type in data_types){
    if (data_type == "bid") {
        col_names <- bid_colnames
    } else {
        col_names <- all_colnames
    }

    ## NOTE: this function actually saves the data for youx
    lapply(data_dates, function(data_date) {
        ipinner::read_split_save(data_type = data_type,
                                 data_date = data_date,
                                 col_names = col_names,
                                 input_folder,
                                 adsplit_folder)
    })
}

## we just used these two for whatever reason
ad_ids <- c("1458", "3386")
exchanges <- c("1", "2", "3")

## we'll create the combined data
for (ad_id in ad_ids) {
    for (exchange in exchanges) {
        combined_data <- ipinner::create_combined_data(
                                      ad_id = ad_id,
                                      exchange = exchange,
                                      input_folder = adsplit_folder)
        data.table::fwrite(combined_data,
                           paste0(adsplit_folder, "/", ad_id, "/",
                                  "combined_exchange", exchange, ".csv"))        
    }
}

## optionally you can now delete the huge extra files we've created
## during this process

##------------------------------------
