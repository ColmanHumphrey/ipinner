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
