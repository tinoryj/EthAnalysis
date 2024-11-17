library(grid)
library(ggplot2)
library(extrafont)
library(scales)
library(ggpmisc) # Ensure ggpmisc is installed
library(data.table)
library(minpack.lm)

if (TRUE) {
    args <- commandArgs(trailingOnly = TRUE)
    x1 <- fread(args[1], header = TRUE)
    # remove NA, NaN or Inf lines
    if (is.list(x1)) {
        x1 <- as.data.table(x1)
    }

    x1 <- x1[complete.cases(x1)]
    x1 <- x1[x1$ID > 0 & x1$Count > 0]
    x1 <- x1[!is.infinite(x1$ID) & !is.infinite(x1$Count)]


    # Fit the Zipf distribution model
    zipf_model <- nls(
        Count ~ C * ID^(-alpha),
        data = x1,
        start = list(C = max(x1$Count), alpha = 0)
    )
    coef_zipf <- coef(zipf_model)
    formula_text <- paste(
        "y = ", round(coef_zipf["C"], 2), "*x^(-", round(coef_zipf["alpha"], 2), ")",
        sep = ""
    )

    r_squared <- paste("R² = ", round(1 - sum(residuals(zipf_model)^2) / sum((x1$Count - mean(x1$Count))^2), 2))

    cat("Fitted Formula for file: ", args[1], "\n")
    cat(formula_text, "\n\n")
    cat(r_squared, "\n")
}
