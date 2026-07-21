# =========================================================
# Semantic drift framework figure
# Figure-only version (no companion panel)
# =========================================================

# Install if needed
if (!requireNamespace("ggplot2", quietly = TRUE)) install.packages("ggplot2")
if (!requireNamespace("ggtext", quietly = TRUE)) install.packages("ggtext")

library(ggplot2)
library(ggtext)
library(grid)

# -------------------------
# Palette aligned to TikZ diagram
# -------------------------
col_blue_fill   <- "#CCCCFF"  # blue!20
col_blue_line   <- "#8080FF"  # blue!50
col_yellow_fill <- "#FFFFCC"  # yellow!20
col_yellow_line <- "#999900"  # yellow!60!black
col_red_fill    <- "#FFCCCC"  # red!20
col_red_line    <- "#FF8080"  # approx. red!50

# -------------------------
# Geometry setup
# -------------------------

# Basis vectors for the three dimensions
b <- c(3.95, 0.0)   # Breadth axis (horizontal)
i <- c(0.0, 3.45)   # Intensity axis (vertical)
s <- c(2.95, 2.2)   # Sentiment axis (diagonal)

# Helper to build a plane spanned by two vectors
make_plane <- function(v1, v2) {
  data.frame(
    x = c(-v1[1] - v2[1],  v1[1] - v2[1],  v1[1] + v2[1], -v1[1] + v2[1]),
    y = c(-v1[2] - v2[2],  v1[2] - v2[2],  v1[2] + v2[2], -v1[2] + v2[2])
  )
}

# Planes
plane_breadth   <- make_plane(b, i)  # breadth dimension -> blue
plane_intensity <- make_plane(s, i)  # intensity dimension -> yellow
plane_sentiment <- make_plane(b, s)  # sentiment dimension -> red

# Axis endpoints
breadth_end   <- 1.18 * b
intensity_end <- 1.18 * i
sentiment_end <- 1.22 * s

# -------------------------
# Labels
# -------------------------

endpoint_labels <- data.frame(
  x = c(-4.95,  5.20,  0.12,  0.12, -5.00,  4.55),
  y = c( 1.05,  1.05,  5.18, -5.10, -2.10,  3.18),
  label = c(
    "<b>Narrower</b><br><span style='font-size:10pt'><i>Gay</i></span>",
    "<b>Broader</b><br><span style='font-size:10pt'><i>Holiday</i></span>",
    "<b>Stronger</b><br><span style='font-size:10pt'><i>Kill</i></span>",
    "<b>Weaker</b><br><span style='font-size:10pt'><i>Awesome</i></span>",
    "<b>More negative</b><br><span style='font-size:10pt'><i>Silly</i></span>",
    "<b>More positive</b><br><span style='font-size:10pt'><i>Nice</i></span>"
  )
)

# -------------------------
# Plot
# -------------------------

p <- ggplot() +
  # Planes
  geom_polygon(
    data = plane_breadth, aes(x, y),
    fill = col_blue_fill, alpha = 0.18,
    colour = col_blue_line, linewidth = 0.65
  ) +
  geom_polygon(
    data = plane_intensity, aes(x, y),
    fill = col_yellow_fill, alpha = 0.20,
    colour = col_yellow_line, linewidth = 0.65
  ) +
  geom_polygon(
    data = plane_sentiment, aes(x, y),
    fill = col_red_fill, alpha = 0.18,
    colour = col_red_line, linewidth = 0.65
  ) +
  
  # Axes
  annotate(
    "segment",
    x = -breadth_end[1], y = 0,
    xend = breadth_end[1], yend = 0,
    arrow = arrow(length = unit(0.26, "cm"), type = "closed"),
    linewidth = 1.15, colour = "grey15"
  ) +
  annotate(
    "segment",
    x = 0, y = -intensity_end[2],
    xend = 0, yend = intensity_end[2],
    arrow = arrow(length = unit(0.26, "cm"), type = "closed"),
    linewidth = 1.15, colour = "grey15"
  ) +
  annotate(
    "segment",
    x = -sentiment_end[1], y = -sentiment_end[2],
    xend = sentiment_end[1], yend = sentiment_end[2],
    arrow = arrow(length = unit(0.26, "cm"), type = "closed"),
    linewidth = 1.15, colour = "grey15"
  ) +
  
  # Axis titles
  annotate(
    "text", x = 0.02, y = 4.42, label = "Intensity",
    fontface = "bold", size = 6.3, colour = "grey10"
  ) +
  annotate(
    "text", x = 5.62, y = 0.34, label = "Breadth",
    fontface = "bold", size = 6.0, colour = "grey10", hjust = 0
  ) +
  annotate(
    "text", x = -4.55, y = -4.08, label = "Sentiment",
    fontface = "bold", size = 6.0, colour = "grey10", hjust = 0
  ) +
  
  # Endpoint labels
  geom_richtext(
    data = endpoint_labels,
    aes(x = x, y = y, label = label),
    hjust = 0.5, vjust = 0.5,
    fill = "white",
    label.colour = "#7A7A7A",
    colour = "grey10",
    size = 5.0,
    label.size = 0.28,
    label.padding = unit(c(0.10, 0.16, 0.10, 0.16), "lines"),
    label.r = unit(0.10, "lines"),
    inherit.aes = FALSE
  ) +
  
  coord_fixed(
    xlim = c(-6.15, 6.55),
    ylim = c(-5.75, 5.75),
    clip = "off"
  ) +
  
  theme_void(base_size = 16) +
  theme(
    plot.background  = element_rect(fill = "white", colour = NA),
    panel.background = element_rect(fill = "white", colour = NA),
    plot.margin      = margin(12, 12, 12, 12)
  )

print(p)

ggsave(
  filename = file.path(getwd(), "semantic_drift_framework_final.png"),
  plot = p,
  width = 10.2,
  height = 7.1,
  dpi = 320,
  bg = "white"
)

ggsave(
  filename = file.path(getwd(), "semantic_drift_framework_final.pdf"),
  plot = p,
  width = 10.2,
  height = 7.1,
  bg = "white"
)