library(dplyr)

run_analysis <- function(conf_data) {
    # Arbitrary code -----------------------------------------------------------
    transformed_df <- conf_data %>% 
        mutate(agi_above_30k = case_when(ADJGINC > 30000 ~ 1, 
                                         TRUE ~ 0))
    
    # Specify analyses -----------------------------------------------------------
    # Example linear model 
    lm_fit <- lm(ADJGINC ~ AGE, data = transformed_df)
    lm_example <- get_model_output(
        fit = lm_fit, 
        model_name = "Example Linear Model"
    )
    
    # Example binomial model 
    glm_fit <- glm(agi_above_30k ~ AGE, family = binomial, data = transformed_df)
    glm_example <- get_model_output(
        fit = glm_fit, 
        model_name = "Example Binomial Model"
    )
    
    # Submit analyses ------------------------------------------------------------
    submit_output(lm_example, glm_example)
}