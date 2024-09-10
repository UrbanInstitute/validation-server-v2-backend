library(dplyr)

run_analysis <- function(conf_data) {
    # Arbitrary code -----------------------------------------------------------
    transformed_df <- conf_data %>%
        filter(AGE >= 18, AGE <= 65) %>% 
        mutate(earned_income = INCWAGE + INCBUS + INCFARM) %>% 
        mutate(MARST = as.factor(MARST), 
               SEX = as.factor(SEX))
    
    # Specify analyses -----------------------------------------------------------
    # Example summary statistic  
    table1 <- get_table_output(
        data = transformed_df,
        table_name = "Example Table 1",
        stat = "mean",
        var = "earned_income"
    )
    
    # Example table with multiple stat/var/by values 
    table2 <- get_table_output(
        data = transformed_df,
        table_name = "Example Table 2",
        stat = c("mean", "sd"),
        var = c("earned_income", "ADJGINC"), 
        by = c("MARST", "SEX")
    )
    
    # Submit analyses ------------------------------------------------------------
    submit_output(table1, table2)
}
