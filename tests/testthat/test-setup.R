context("test-setup")

test_that("testing read_split_save", {
    random_folder <- paste0(sample(letters, 30, TRUE), collapse = "")

    ## should fail for "bad" input
    expect_error(read_split_save(data_type = "this_doesn't_exist",
                                 data_date = "20130608",
                                 col_names = letters,
                                 input_folder = random_folder,
                                 output_folder = random_folder))
    expect_error(read_split_save(data_type = "bid",
                                 data_date = "20130608",
                                 col_names = letters,
                                 input_folder = random_folder,
                                 output_folder = random_folder),
                 NA)
    expect_error(read_split_save(data_type = "clk",
                                 data_date = "20131308", # no month 13!
                                 col_names = letters,
                                 input_folder = random_folder,
                                 output_folder = random_folder))
    expect_error(read_split_save(data_type = "clk",
                                 data_date = "not_a_date",
                                 col_names = letters,
                                 input_folder = random_folder,
                                 output_folder = random_folder))
    expect_error(read_split_save(data_type = "clk",
                                 data_date = "20130608",
                                 col_names = letters,
                                 input_folder = random_folder,
                                 output_folder = random_folder),
                 NA)

    ## returns false for files that don't exist
    expect_false(read_split_save(data_type = "conv",
                                 data_date = "20130608",
                                 col_names = letters,
                                 input_folder = random_folder,
                                 output_folder = random_folder))
})

test_that("testing coalesce", {
    vec_main <- sample(0:9, 1000, TRUE)
    vec_1 <- vec_main
    vec_2 <- vec_main
    vec_3 <- vec_main

    vec_1[c(1:10, sample(100, 20) + 100)] <- NA
    vec_2[c(1:10, sample(100, 20) + 200)] <- NA
    vec_3[c(1:5, sample(100, 20) + 300)] <- NA

    ret_vec <- coalesce(vec_1, vec_2, vec_3)

    expect_equal(ret_vec[6:1000], vec_main[6:1000])
    expect_true(all(is.na(ret_vec[1:5])))


    expect_error(coalesce(vec_1, vec_2[1:500]))
})

test_that("testing sawtooth", {
    input_vec <- rep(letters[1:6],
                        times = c(3, 3, 1, 1, 2, 2))
    expect_equal(sawtooth(input_vec),
                 c(1, 2, 3, 1, 2, 3, 1, 1, 1, 2, 1, 2))

    ## not considered equal
    expect_equal(sawtooth(c(NA, NA, NA)),
                 c(1, 1, 1))
})
