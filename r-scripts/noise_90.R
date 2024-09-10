library(tidyverse)

theta <- 100 
chi <- 10 
N <- 1000
omega <- quantile(abs(rnorm(100)), probs = c(0.9))
noise_90 <- (sqrt(2) * chi / N * omega) 

epsilon <- seq(0, 10)
error <- noise_90 / epsilon

df <- data.frame(
    epsilon, 
    error, 
    min = theta - error, 
    max = theta + error
)

ggplot(df, aes(x = epsilon, y = theta, ymin = min, ymax = max)) + 
    geom_line(linetype = "dashed") + 
    geom_ribbon(alpha = 0.2)
