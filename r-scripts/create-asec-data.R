# Downloads data sample used for the Urban Institute's Validation Server Prototype  
# Includes available income, tax, and demographic variables from 2022 & 2023 CPS-ASEC 

library(dplyr)
library(ipumsr)

# Define extract request 
income_vars <- c(
    "FTOTVAL",  "INCTOT",   "INCWAGE",  "INCBUS",   "INCFARM",  "INCSS", 
    "INCWELFR", "INCRETIR", "INCSSI",   "INCINT",   "INCUNEMP", "INCWKCOM", 
    "INCVET",   "INCSURV",  "INCDISAB", "INCDIVID", "INCRENT",  "INCEDUC",  
    "INCCHILD", "INCASIST", "INCOTHER", "INCRANN",  "INCPENS",  "INCLONGJ", 
    "OINCBUS",  "OINCFARM", "OINCWAGE", "SRCSURV1", "SRCSURV2", "INCSURV1", 
    "INCSURV2", "SRCDISA1", "SRCDISA2", "INCDISA1", "INCDISA2", "SRCRET1",  
    "SRCRET2",  "INCRET1",  "INCRET2",  "SRCPEN1",  "SRCPEN2",  "INCPEN1",  
    "INCPEN2",  "RETCONT",  "SRCRINT1", "SRCRINT2", "INCRINT1", "INCRINT2", 
    "INCCAPG",  "SRCEARN",  "SRCEDUC",  "SRCUNEMP", "SRCWELFR", "SRCWKCOM", 
    "MTHWELFR", "VETQA",    "WHYSS1",   "WHYSS2",   "WHYSSI1",  "WHYSSI2", 
    "GOTVDISA", "GOTVEDUC", "GOTVOTHE", "GOTVPENS", "GOTVSURV"
)

tax_vars <- c(
    "CTCCRD",   "ACTCCRD", "ADJGINC", "EITCRED",  "FEDTAX",   "FEDTAXAC", "FICA",
    "FILESTAT", "DEPSTAT", "MARGTAX", "STATETAX", "STATAXAC", "TAXINC"
)
          
demographic_vars <- c(
    "RELATE", "AGE", "SEX", "RACE", "MARST", "POPSTAT", "ASIAN", "VETSTAT"
)
                  
# Submit, download, and read extract 
data <- define_extract_cps(
    description = "ASEC Extract for Validation Server Prototype",
    samples = c("cps2022_03s", "cps2023_03s"), 
    variables = c(income_vars, tax_vars, demographic_vars)
) %>% 
    submit_extract() %>%
    wait_for_extract() %>%
    download_extract() %>%
    read_ipums_micro()

# Write to csv  
write.csv(data, "cps_2022-2023.csv", row.names = FALSE)
