library(grid)
library(ggplot2)
library(extrafont)
library(scales)
library(ggpmisc) # Ensure ggpmisc is installed
library(dplyr)

mywidth <- 10
myheight <- 5
colorManual <- c("#C1121F")
my_line <- c("solid")

# Set the maximum polynomial degree
max_degree <- 6

if (TRUE) {
    args <- commandArgs(trailingOnly = TRUE)
    x1 <- read.table(args[1], header = TRUE)
    x1_filtered <- x1 %>% filter(count != 0)

    cairo_pdf(file = args[2], width = mywidth, height = myheight)
    ggplot(data = x1_filtered, aes(x = bucket, y = count)) +
        geom_point(size = 4, color = colorManual) +
        scale_y_continuous(
            expand = c(0, 0), labels = scales::comma,
            limits <- c(0, max(x1$count) * 1.3) # Corrected usage
        ) +
        scale_x_continuous(
            expand = c(0, 0), labels = scales::comma,
            limits <- c(0, max(x1$bucket) * 1.3) # Corrected usage
        ) +
        ylab("Appear Frequency") +
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
            # legend.title = element_blank(),
            legend.position = "none",
            legend.margin = margin(t = 0, unit = "cm"),
            legend.direction = "horizontal",
            legend.box = "horizontal",
            legend.text = element_text(size = 16.5),
            plot.margin = unit(c(0.1, 0.1, 0.1, 0.1), "cm")
        )
}
