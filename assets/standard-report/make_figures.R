#!/usr/bin/env Rscript
# Generate W1-W6 figures from BioAgent's curated tables. Missing inputs are skipped.
suppressPackageStartupMessages({
  library(ggplot2)
  library(dplyr)
  library(readr)
  library(scales)
})

args <- commandArgs(trailingOnly = TRUE)
table_dir <- if (length(args) >= 1) args[[1]] else "tables"
fig_dir <- if (length(args) >= 2) args[[2]] else "figures"
command <- commandArgs(FALSE)
file_arg <- sub("^--file=", "", command[grepl("^--file=", command)])
script_dir <- if (length(file_arg)) dirname(normalizePath(file_arg[[1]])) else getwd()
source(file.path(script_dir, "00_sci_theme.R"), local = FALSE)
dir.create(fig_dir, recursive = TRUE, showWarnings = FALSE)

master <- safe_read(file.path(table_dir, "master_samples.tsv"))
if (is.null(master)) quit(save = "no", status = 0)
core <- master
if ("in_core_cohort" %in% names(master)) {
  selected <- master %>% filter(in_core_cohort %in% c(TRUE, "True", "true", 1, "1"))
  if (nrow(selected) > 0) core <- selected
}

group_column <- NULL
for (candidate in c("method", "group", "treatment", "condition", "library", "batch")) {
  if (candidate %in% names(core) && n_distinct(na.omit(core[[candidate]])) >= 2) {
    group_column <- candidate
    break
  }
}

point_or_distribution <- function(frame, y, ylab, ypercent = FALSE, log_scale = FALSE) {
  data <- frame %>% filter(is.finite(.data[[y]]))
  if (nrow(data) == 0) return(NULL)
  if (!is.null(group_column)) {
    data$plot_group <- factor(data[[group_column]])
  } else {
    data$plot_group <- factor("all")
  }
  plot <- ggplot(data, aes(x = plot_group, y = .data[[y]], colour = plot_group))
  counts <- table(data$plot_group)
  if (nrow(data) >= 4 && sum(counts >= 2) >= 2) {
    plot <- plot + geom_boxplot(aes(fill = plot_group), width = 0.55, outlier.shape = NA, alpha = 0.2)
  }
  plot <- plot + geom_point(
    position = position_jitter(width = if (nrow(data) > 1) 0.08 else 0, height = 0, seed = 1),
    size = 2.2
  ) +
    labs(x = NULL, y = ylab) + theme_sci() +
    theme(axis.text.x = element_text(angle = 20, hjust = 1), legend.position = "none") +
    scale_colour_manual(values = setNames(okabe_ito[seq_along(levels(data$plot_group))], levels(data$plot_group)))
  if (ypercent) plot <- plot + scale_y_continuous(labels = percent_format(accuracy = 1))
  if (log_scale && all(data[[y]] > 0)) plot <- plot + scale_y_log10(labels = comma)
  plot
}

if (has_cols(core, c("sample_id", "merge_r1_reads"))) {
  data <- core %>% filter(is.finite(merge_r1_reads)) %>% mutate(depth = merge_r1_reads / 1e6)
  if (nrow(data) > 0) {
    plot <- ggplot(data, aes(x = reorder(sample_id, depth), y = depth)) +
      geom_col(fill = "#0072B2", colour = "black", linewidth = 0.2, width = 0.72) +
      geom_hline(yintercept = 5, linetype = "dashed", linewidth = 0.3, colour = "#D55E00") +
      coord_flip() + labs(x = NULL, y = "Merged R1 reads (millions)", caption = "Dashed line: 5 million reads") +
      theme_sci()
    save_panel(plot, file.path(fig_dir, "fig01_depth_by_sample"), 114, 90)
  }
}

if (has_cols(core, "read_retention_fastp_median")) {
  data <- core
  if (max(data$read_retention_fastp_median, na.rm = TRUE) > 1.5) {
    data$read_retention_fastp_median <- data$read_retention_fastp_median / 100
  }
  plot <- point_or_distribution(data, "read_retention_fastp_median", "fastp read retention", TRUE)
  if (!is.null(plot)) save_panel(plot, file.path(fig_dir, "fig02_w1_fastp_retention"))
}
if (has_cols(core, "q30_after_fastp_median")) {
  data <- core %>% mutate(q30_plot = ifelse(q30_after_fastp_median > 1.5, q30_after_fastp_median / 100, q30_after_fastp_median))
  plot <- point_or_distribution(data, "q30_plot", "fastp Q30", TRUE)
  if (!is.null(plot)) save_panel(plot, file.path(fig_dir, "fig03_w1_q30"))
}
if (has_cols(core, "rrna_retention_r1_median")) {
  data <- core %>% mutate(rrna_plot = ifelse(rrna_retention_r1_median > 1.5, rrna_retention_r1_median / 100, rrna_retention_r1_median))
  plot <- point_or_distribution(data, "rrna_plot", "R1 retained after rRNA depletion", TRUE)
  if (!is.null(plot)) save_panel(plot, file.path(fig_dir, "fig04_w1_rrna_retention"))
}
if (has_cols(core, c("sample_id", "merge_r1_reads", "contigs_ge1kb_n"))) {
  data <- core %>% filter(is.finite(merge_r1_reads), is.finite(contigs_ge1kb_n))
  if (nrow(data) > 0) {
    plot <- ggplot(data, aes(x = merge_r1_reads / 1e6, y = contigs_ge1kb_n)) +
      geom_point(size = 2.4, colour = "#0072B2") +
      geom_text(aes(label = sample_id), size = 2.2, vjust = -0.8) +
      labs(x = "Merged R1 reads (millions)", y = "Contigs >=1 kb (n)") + theme_sci()
    save_panel(plot, file.path(fig_dir, "fig05_w1_depth_vs_contigs"), 114, 80)
  }
}

contigs <- safe_read(file.path(table_dir, "extracted", "w2__contigs.tsv"))
if (has_cols(contigs, "checkv_quality")) {
  if ("source_sample" %in% names(contigs)) contigs$sample_id <- contigs$source_sample
  if ("sample_id" %in% names(contigs)) {
    contigs$sample_id <- trimws(as.character(contigs$sample_id))
    contigs$sample_id[tolower(contigs$sample_id) %in% c("", "na", "nan", "none", "null")] <- NA_character_
    if (all(is.na(contigs$sample_id)) && nrow(core) == 1) contigs$sample_id <- core$sample_id[[1]]
    quality <- contigs %>%
      filter(!is.na(sample_id), !is.na(checkv_quality), checkv_quality != "") %>%
      count(sample_id, checkv_quality, name = "n")
    if (nrow(quality) > 0) {
      plot <- ggplot(quality, aes(x = sample_id, y = n, fill = checkv_quality)) +
        geom_col(width = 0.8, colour = "black", linewidth = 0.15) +
        scale_fill_manual(values = okabe_ito, name = "CheckV") +
        labs(x = NULL, y = "Putative viral contigs (n)") + theme_sci() +
        theme(axis.text.x = element_text(angle = 45, hjust = 1), legend.position = "bottom")
      save_panel(plot, file.path(fig_dir, "fig06_w2_checkv_by_sample"), 114, 80)
    }
  }
}
if (has_cols(contigs, "length")) {
  data <- contigs %>% filter(is.finite(length), length > 0)
  if (nrow(data) > 0) {
    if (nrow(data) == 1) {
      plot <- ggplot(data, aes(x = "contig", y = length)) + geom_col(fill = "#0072B2", width = 0.55) +
        labs(x = NULL, y = "Putative viral contig length (bp)") + theme_sci()
    } else {
      plot <- ggplot(data, aes(x = length)) + geom_histogram(bins = min(20, nrow(data)), fill = "#0072B2", colour = "white") +
        scale_x_log10(labels = comma) + labs(x = "Putative viral contig length (bp, log10)", y = "Count") + theme_sci()
    }
    save_panel(plot, file.path(fig_dir, "fig07_w2_contig_length"))
  }
}
if (has_cols(core, "n_putative_viral_contigs")) {
  plot <- point_or_distribution(core, "n_putative_viral_contigs", "Putative viral contigs per sample (n)")
  if (!is.null(plot)) save_panel(plot, file.path(fig_dir, "fig08_w2_viral_contigs_by_method"))
}

host <- safe_read(file.path(table_dir, "extracted", "w3__host_top_species.tsv"))
if (has_cols(host, "inferred_mt_species")) {
  species <- host %>% count(inferred_mt_species, name = "n") %>% arrange(desc(n)) %>% slice_head(n = 12)
  plot <- ggplot(species, aes(x = reorder(inferred_mt_species, n), y = n)) + geom_col(fill = "#009E73") +
    coord_flip() + labs(x = NULL, y = "Samples (n)") + theme_sci()
  save_panel(plot, file.path(fig_dir, "fig09_w3_host_species"), 114, 80)
}
if (has_cols(core, "kraken_unclassified_frac")) {
  data <- core %>% mutate(unclassified_plot = ifelse(kraken_unclassified_frac > 1.5, kraken_unclassified_frac / 100, kraken_unclassified_frac))
  plot <- point_or_distribution(data, "unclassified_plot", "Kraken unclassified fraction", TRUE)
  if (!is.null(plot)) save_panel(plot, file.path(fig_dir, "fig10_w4_unclassified"))
}
if (has_cols(core, "bracken_species_taxa")) {
  plot <- point_or_distribution(core, "bracken_species_taxa", "Bracken species taxa (n)")
  if (!is.null(plot)) save_panel(plot, file.path(fig_dir, "fig11_w4_species_richness"))
}
top_taxa <- safe_read(file.path(table_dir, "extracted", "w4__bracken_top15_species.tsv"))
if (has_cols(top_taxa, c("sample_id", "name", "fraction_total_reads"))) {
  plot <- ggplot(top_taxa, aes(x = sample_id, y = fraction_total_reads, fill = name)) + geom_col() +
    scale_y_continuous(labels = percent_format(accuracy = 1)) + labs(x = NULL, y = "Fraction of total reads", fill = NULL) +
    theme_sci() + theme(axis.text.x = element_text(angle = 45, hjust = 1), legend.position = "bottom")
  save_panel(plot, file.path(fig_dir, "fig12_w4_top_taxa"), 174, 95)
}
if (has_cols(core, "mapping_rate_pct")) {
  data <- core %>% mutate(mapping_plot = ifelse(mapping_rate_pct > 1.5, mapping_rate_pct / 100, mapping_rate_pct))
  plot <- point_or_distribution(data, "mapping_plot", "Reads mapped to viral contigs", TRUE)
  if (!is.null(plot)) save_panel(plot, file.path(fig_dir, "fig13_w5_mapping_rate"))
}
if (has_cols(core, "max_rpm")) {
  plot <- point_or_distribution(core, "max_rpm", "Maximum viral RPM", FALSE, TRUE)
  if (!is.null(plot)) save_panel(plot, file.path(fig_dir, "fig14_w5_max_rpm"))
}
if (has_cols(core, c("sample_id", "merge_r1_reads", "mapping_rate_pct"))) {
  data <- core %>% filter(is.finite(merge_r1_reads), is.finite(mapping_rate_pct)) %>%
    mutate(mapping_plot = ifelse(mapping_rate_pct > 1.5, mapping_rate_pct / 100, mapping_rate_pct))
  if (nrow(data) > 0) {
    plot <- ggplot(data, aes(x = merge_r1_reads / 1e6, y = mapping_plot)) + geom_point(size = 2.4, colour = "#0072B2") +
      geom_text(aes(label = sample_id), size = 2.2, vjust = -0.8) + scale_y_continuous(labels = percent_format()) +
      labs(x = "Merged R1 reads (millions)", y = "Viral-contig mapping rate") + theme_sci()
    save_panel(plot, file.path(fig_dir, "fig15_w5_depth_vs_mapping"), 114, 80)
  }
}

for (spec in list(
  c("w6_checkv_completeness", "fig16_w6_checkv_quality", "W6 CheckV completeness"),
  c("n_annotated", "fig17_w6_annotation_strategy", "Annotated objects (n)"),
  c("n_genes", "fig18_w6_annotation_outcomes", "Annotated genes (n)")
)) {
  if (has_cols(core, spec[[1]])) {
    plot <- point_or_distribution(core, spec[[1]], spec[[3]])
    if (!is.null(plot)) save_panel(plot, file.path(fig_dir, spec[[2]]))
  }
}
