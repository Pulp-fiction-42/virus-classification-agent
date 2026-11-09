# Journal-style ggplot2 theme used by BioAgent standard delivery reports.

okabe_ito <- c(
  "#E69F00", "#56B4E9", "#009E73", "#F0E442",
  "#0072B2", "#D55E00", "#CC79A7", "#000000"
)

theme_sci <- function(base_size = 8, base_family = "sans") {
  ggplot2::theme_classic(base_size = base_size, base_family = base_family) +
    ggplot2::theme(
      line = ggplot2::element_line(colour = "black", linewidth = 0.35),
      axis.line = ggplot2::element_line(colour = "black", linewidth = 0.35),
      axis.ticks = ggplot2::element_line(colour = "black", linewidth = 0.35),
      axis.text = ggplot2::element_text(colour = "black", size = base_size),
      axis.title = ggplot2::element_text(colour = "black", size = base_size),
      legend.title = ggplot2::element_text(size = base_size),
      legend.text = ggplot2::element_text(size = base_size - 0.5),
      legend.key.size = grid::unit(0.35, "cm"),
      legend.background = ggplot2::element_blank(),
      legend.key = ggplot2::element_blank(),
      strip.background = ggplot2::element_blank(),
      strip.text = ggplot2::element_text(face = "bold", size = base_size),
      plot.title = ggplot2::element_text(size = base_size + 0.5, face = "bold", hjust = 0),
      plot.subtitle = ggplot2::element_text(size = base_size - 0.5, colour = "grey20"),
      plot.caption = ggplot2::element_text(size = base_size - 1, colour = "grey30", hjust = 0),
      plot.background = ggplot2::element_rect(fill = "white", colour = NA),
      panel.background = ggplot2::element_rect(fill = "white", colour = NA),
      panel.grid = ggplot2::element_blank(),
      plot.margin = ggplot2::margin(4, 6, 4, 4)
    )
}

save_panel <- function(plot, stem, width_mm = 85, height_mm = 70, dpi = 300) {
  ggplot2::ggsave(
    paste0(stem, ".png"), plot,
    width = width_mm, height = height_mm, units = "mm", dpi = dpi, bg = "white"
  )
  ggplot2::ggsave(
    paste0(stem, ".pdf"), plot,
    width = width_mm, height = height_mm, units = "mm", bg = "white"
  )
}

safe_read <- function(path) {
  if (!file.exists(path)) return(NULL)
  frame <- tryCatch(readr::read_tsv(path, show_col_types = FALSE), error = function(e) NULL)
  if (is.null(frame) || nrow(frame) == 0) return(NULL)
  frame
}

has_cols <- function(frame, columns) {
  !is.null(frame) && all(columns %in% names(frame))
}
