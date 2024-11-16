library(grid)
library(ggplot2)
library(extrafont)
library(scales)
library(ggpmisc) # Ensure ggpmisc is installed
library(data.table)

mywidth <- 10
myheight <- 5
colorManual <- c("#C1121F")

if (TRUE) {
    args <- commandArgs(trailingOnly = TRUE)
    x1 <- fread(args[1], header = TRUE)

    # Fit the Zipf distribution model
    zipf_model <- nls(
        Count ~ C * ID^(-alpha),
        data = x1,
        start = list(C = max(x1$Count), alpha = 1.0)
    )
    coef_zipf <- coef(zipf_model)
    formula_text <- paste(
        "y = ", round(coef_zipf["C"], 2), "*x^(-", round(coef_zipf["alpha"], 2), ")",
        sep = ""
    )

    r_squared <- paste("R² = ", round(1 - sum(residuals(zipf_model)^2) / sum((x1$Count - mean(x1$Count))^2), 2))

    cairo_pdf(file = args[2], width = mywidth, height = myheight)
    ggplot(data = x1, aes(x = ID, y = Count)) +
        geom_point(size = 1.5, color = colorManual) +
        stat_function(
            fun = function(x) coef_zipf["C"] * x^(-coef_zipf["alpha"]),
            color = "blue",
            linewidth = 1
        ) +
        annotate("text", x = Inf, y = Inf, label = formula_text, hjust = 1.1, vjust = 2, size = 6, color = "blue") +
        annotate("text", x = Inf, y = Inf, label = r_squared, hjust = 1.1, vjust = 4, size = 6, color = "blue") +
        scale_y_continuous(
            expand = c(0, 0), labels = scales::comma,
            limits = c(0, max(x1$Count) * 1.1)
        ) +
        scale_x_continuous(
            expand = c(0, 0), labels = scales::comma,
            limits = c(0, max(x1$ID) * 1.1)
        ) +
        geom_hline(yintercept = 0, linetype = "dashed", color = "black") +
        geom_vline(xintercept = 0, linetype = "dashed", color = "black") +
        ylab("Frequency") +
        xlab("i-th KV pair") +
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
