library(grid)
library(ggplot2)
library(extrafont)
library(scales)
library(ggpmisc) # Ensure ggpmisc is installed

mywidth <- 10
myheight <- 5
colorManual <- c("#C1121F")
my_line <- c("solid")

# Set the maximum polynomial degree
max_degree <- 6

if (TRUE) {
    args <- commandArgs(trailingOnly = TRUE)
    x1 <- read.table(args[1], header = TRUE)

    # Fit the model with the specified degree
    model <- lm(count ~ poly(bucket, max_degree, raw = TRUE), data = x1)

    # Generate the formula text dynamically based on the coefficients
    formula_text <- paste(
        "y = ", paste(
            sapply(0:max_degree, function(i) {
                paste0(round(coef(model)[i + 1], 2), ifelse(i > 0, paste0("*x^", i), ""))
            }),
            collapse = " + "
        ),
        sep = ""
    )

    cat("Fitted Polynomial Formula:\n")
    cat(formula_text, "\n\n")

    # Split formula text into multiple lines
    formula_lines <- strwrap(formula_text, width = 50)
    formula_multiline <- paste(formula_lines, collapse = "\n")

    # Prepare R² value
    r_squared <- paste("R² = ", round(summary(model)$r.squared, 2))

    cairo_pdf(file = args[2], width = mywidth, height = myheight)
    ggplot(data = x1, aes(x = bucket, y = count)) +
        geom_line(linewidth = 2, color = colorManual) +
        geom_smooth(method = "lm", formula = y ~ poly(x, max_degree, raw = TRUE), color = "blue", se = FALSE, linewidth = 1) +
        # Annotate formula and R² with proper positions
        # annotate("text",
        #     x = max(x1$bucket) * 0.6, y = max(x1$count) * 1.05,
        #     label = formula_multiline, hjust = 0, vjust = 1, size = 5, color = "blue"
        # ) +
        # annotate("text",
        #     x = max(x1$bucket) * 0.6, y = max(x1$count) * 0.95,
        #     label = r_squared, hjust = 0, vjust = 1, size = 5, color = "blue"
        # ) +
        scale_y_continuous(
            expand = c(0, 0), labels = scales::comma,
            limits = c(0, max(x1$count) * 1.2) # Adjusted limits for better spacing
        ) +
        scale_x_continuous(
            expand = c(0, 0), labels = scales::comma,
            limits = c(0, max(x1$bucket) * 1.3) # Adjusted limits for better spacing
        ) +
        ylab("Frequency") +
        xlab("KV size") +
        theme_bw() +
        theme(
            panel.grid.major = element_blank(), panel.grid.minor = element_blank(),
            panel.background = element_blank(),
            panel.border = element_blank(),
            axis.line = element_line(colour = "black", linewidth = 0.15),
            axis.ticks = element_line(linewidth = 0.15),
            axis.text.x = element_text(margin = margin(5, 0, 0, 0), angle = 0, hjust = 0.5, colour = "black", size = 20),
            axis.title.y = element_text(size = 19, hjust = 0.5),
            axis.text.y = element_text(margin = margin(0, 2, 0, 0), colour = "black", size = 20),
            axis.title.x = element_text(size = 20),
            legend.key.size = unit(0.5, "cm"),
            legend.title = element_blank(),
            legend.position = "none",
            legend.margin = margin(t = 0, unit = "cm"),
            legend.direction = "horizontal",
            legend.box = "horizontal",
            legend.text = element_text(size = 16.5),
            plot.margin = unit(c(0.1, 0.1, 0.1, 0.1), "cm")
        )
}
