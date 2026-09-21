# ============================================================
# SCRAPING COMPLETO: r/AutismInWomen
# Fuente:    Arctic Shift API (sin autenticación)
# Periodo:   2025-01-01 → 2026-05-29 (fecha fija; ver DATE_END)
# Paginación: cursor por created_utc del último post
# Requisitos: httr2, jsonlite, dplyr, lubridate
# ============================================================

library(httr2)
library(jsonlite)
library(dplyr)
library(lubridate)

SUBREDDIT  <- "AutismInWomen"
BASE_URL   <- "https://arctic-shift.photon-reddit.com/api/posts/search"
DATE_START <- "2025-01-01"
# Fecha fija para que la extracción sea reproducible.
# El corpus original se extrajo el 2026-05-29 (último post: 2026-05-29 17:19 UTC).
DATE_END   <- "2026-05-29"
LIMIT      <- 100
SLEEP_SEC  <- 1.5
OUTPUT_DIR <- "."                       # el CSV crudo se queda en local (no se sube a GitHub)
IDS_DIR    <- file.path("data", "ids")  # esto SÍ se sube
dir.create(IDS_DIR, recursive = TRUE, showWarnings = FALSE)

cat("================================================\n")
cat("  Scraping r/", SUBREDDIT, "\n")
cat("  Periodo:", DATE_START, "→", DATE_END, "\n")
cat("================================================\n\n")

# ── Función de una sola petición ─────────────────────────────
fetch_page <- function(after_ts, before_date) {
  req <- request(BASE_URL) |>
    req_url_query(
      subreddit = SUBREDDIT,
      after     = after_ts,     # timestamp Unix como cursor
      before    = before_date,
      limit     = LIMIT,
      sort_type = "created_utc",
      sort      = "asc"
    ) |>
    req_headers(`User-Agent` = "tfm_autism_research:v1.0") |>
    req_retry(max_tries = 3, backoff = ~ 10)
  
  resp <- req |>
    req_error(is_error = \(r) FALSE) |>
    req_perform()
  
  status <- resp_status(resp)
  if (status != 200) {
    cat("  [AVISO] HTTP", status, "— esperando 30s...\n")
    Sys.sleep(30)
    return(NULL)
  }
  
  datos <- resp_body_json(resp, simplifyVector = TRUE)
  return(datos$data)
}

# ── Función de limpieza ───────────────────────────────────────
clean_posts <- function(df) {
  if (is.null(df) || nrow(df) == 0) return(NULL)
  
  ensure_col <- function(df, col) {
    if (!col %in% names(df)) df[[col]] <- NA
    df
  }
  
  df |>
    ensure_col("selftext") |>
    ensure_col("link_flair_text") |>
    ensure_col("author_flair_text") |>
    ensure_col("score") |>
    ensure_col("num_comments") |>
    ensure_col("upvote_ratio") |>
    ensure_col("url") |>
    ensure_col("permalink") |>
    ensure_col("is_self") |>
    ensure_col("over_18") |>
    ensure_col("spoiler") |>
    ensure_col("distinguished") |>
    transmute(
      id            = id,
      title         = title,
      text          = selftext,
      author        = author,
      created_utc   = as.numeric(created_utc),
      date          = format(as.POSIXct(as.numeric(created_utc),
                                        origin = "1970-01-01", tz = "UTC"),
                             "%Y-%m-%d %H:%M"),
      score         = as.integer(score),
      num_comments  = as.integer(num_comments),
      upvote_ratio  = as.numeric(upvote_ratio),
      post_flair    = link_flair_text,
      author_flair  = author_flair_text,
      url           = url,
      permalink     = paste0("https://reddit.com", permalink),
      is_self_post  = as.logical(is_self),
      is_nsfw       = as.logical(over_18),
      is_spoiler    = as.logical(spoiler),
      distinguished = distinguished,
      has_text      = !is.na(selftext) &
        selftext != "" &
        selftext != "[removed]" &
        selftext != "[deleted]"
    )
}

# ── Loop de paginación por meses ─────────────────────────────
# Estrategia: dentro de cada mes, el cursor es el created_utc
# del último post recibido. Así avanzamos sin repetir posts.

meses <- seq(from = as.Date(DATE_START),
             to   = as.Date(DATE_END),
             by   = "month")

todos_los_posts <- list()
total_llamadas  <- 0
total_posts     <- 0

for (i in seq_along(meses)) {
  
  mes_inicio <- meses[i]
  mes_fin    <- if (i < length(meses)) meses[i + 1] else as.Date(DATE_END) + 1
  
  # Timestamps Unix para los límites del mes
  ts_inicio <- as.numeric(as.POSIXct(mes_inicio, tz = "UTC"))
  ts_fin    <- as.numeric(as.POSIXct(mes_fin,    tz = "UTC"))
  
  cat(sprintf("── Mes %02d/%02d: %s\n", i, length(meses),
              format(mes_inicio, "%Y-%m")))
  
  cursor_ts    <- ts_inicio   # empieza al inicio del mes
  posts_mes    <- list()
  pagina       <- 1
  sigue        <- TRUE
  
  while (sigue) {
    cat(sprintf("   Página %d (desde %s)...",
                pagina,
                format(as.POSIXct(cursor_ts, origin="1970-01-01", tz="UTC"),
                       "%Y-%m-%d %H:%M")))
    
    page_data <- fetch_page(
      after_ts    = cursor_ts,
      before_date = format(mes_fin, "%Y-%m-%d")
    )
    
    total_llamadas <- total_llamadas + 1
    
    if (is.null(page_data) || nrow(page_data) == 0) {
      cat(" sin más posts\n")
      sigue <- FALSE
      next
    }
    
    page_clean <- clean_posts(page_data)
    posts_mes[[pagina]] <- page_clean
    
    n_pagina <- nrow(page_clean)
    cat(sprintf(" %d posts\n", n_pagina))
    
    if (n_pagina < LIMIT) {
      # Menos de 100 → fin del mes
      sigue <- FALSE
    } else {
      # El cursor avanza al created_utc del último post
      # +1 segundo para no repetirlo en la siguiente llamada
      ultimo_utc <- page_data$created_utc[nrow(page_data)]
      nuevo_cursor <- as.numeric(ultimo_utc) + 1
      
      if (nuevo_cursor >= ts_fin) {
        # Hemos llegado al límite del mes
        sigue <- FALSE
      } else {
        cursor_ts <- nuevo_cursor
        pagina    <- pagina + 1
        Sys.sleep(SLEEP_SEC)
      }
    }
  }
  
  if (length(posts_mes) > 0) {
    posts_mes_df <- bind_rows(posts_mes)
    total_posts  <- total_posts + nrow(posts_mes_df)
    todos_los_posts[[i]] <- posts_mes_df
    cat(sprintf("   → %d posts (acumulado: %d)\n\n",
                nrow(posts_mes_df), total_posts))
  } else {
    cat("   → 0 posts\n\n")
  }
  
  Sys.sleep(SLEEP_SEC)
}

# ── Consolidar y deduplicar ───────────────────────────────────
cat("Consolidando corpus...\n")

corpus <- bind_rows(todos_los_posts) |>
  distinct(id, .keep_all = TRUE) |>
  arrange(created_utc)

corpus_text <- corpus |> filter(has_text)

cat(sprintf("Posts únicos totales: %d\n", nrow(corpus)))
cat(sprintf("Posts con texto:      %d (%.1f%%)\n",
            nrow(corpus_text),
            100 * nrow(corpus_text) / nrow(corpus)))
cat(sprintf("Llamadas a la API:    %d\n\n", total_llamadas))

# ── Guardar ──────────────────────────────────────────────────
# CSV crudo: contiene texto y autores. NO subir a GitHub (.gitignore).
# El nombre coincide con el que lee step1_EDA_reddit_corpus.Rmd.
file_raw <- file.path(OUTPUT_DIR, "reddit_scrape_raw.csv")
write.csv(corpus, file_raw, row.names = FALSE, fileEncoding = "UTF-8")
cat("Corpus crudo:", file_raw, "\n")

# Lista de IDs: sin texto ni autor. Esto es lo que se sube al repo.
write.csv(
  corpus |> select(id, created_utc, has_text),
  file.path(IDS_DIR, "reddit_post_ids.csv"),
  row.names = FALSE
)

# Log de extracción
write.csv(
  data.frame(
    run_date      = as.character(Sys.Date()),
    subreddit     = SUBREDDIT,
    date_start    = DATE_START,
    date_end      = DATE_END,
    n_posts       = nrow(corpus),
    n_posts_text  = nrow(corpus_text),
    api_calls     = total_llamadas,
    first_post    = min(corpus$date),
    last_post     = max(corpus$date)
  ),
  file.path(IDS_DIR, "extraction_log.csv"),
  row.names = FALSE
)

# ── Resumen final ─────────────────────────────────────────────
cat("\n════════════════════════════════════════════════\n")
cat("  RESUMEN FINAL\n")
cat("════════════════════════════════════════════════\n")
cat("Periodo cubierto:", min(corpus$date), "→", max(corpus$date), "\n")
cat("Posts totales:   ", nrow(corpus), "\n")
cat("Posts con texto: ", nrow(corpus_text), "\n")

cat("\nFlairs más frecuentes:\n")
corpus |>
  count(post_flair, sort = TRUE) |>
  mutate(pct = round(100 * n / sum(n), 1)) |>
  head(10) |>
  as.data.frame() |>
  print()

cat("\nPosts por mes:\n")
corpus |>
  mutate(month = substr(date, 1, 7)) |>
  count(month) |>
  as.data.frame() |>
  print()