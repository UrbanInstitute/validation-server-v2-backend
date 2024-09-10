library(dplyr)

run_analysis <- function(conf_data) {
    # Arbitrary code -----------------------------------------------------------
    transformed_df <- conf_data %>% 
        filter(TYPO == 1) %>% 
        mutate(earned_income = INCWAGE + INCBUS + INCFARM) %>% 
        mutate(MARST = as.factor(MARST)) 
    
    # Specify analyses -----------------------------------------------------------
    # Example regression 
    example_fit <- lm(earned_income ~ MARST + AGE, data = transformed_df)
    example_model <- get_model_output(
        fit = example_fit, 
        model_name = "Example Model"
    )
    
    # Example table 
    example_table <- get_table_output(
        data = transformed_df,
        table_name = "Example Table",
        stat = c("mean", "n"),
        by = "MARST",
        var = "earned_income"
    )
    
    # Submit analyses ------------------------------------------------------------
    submit_output(example_model, example_table)
}
