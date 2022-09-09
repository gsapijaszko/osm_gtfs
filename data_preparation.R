#data preparation

library(tidytransit)
library("DBI")
con = dbConnect(duckdb::duckdb(), dbdir="data/duckdb", read_only=FALSE)

if(!dir.exists("data")) {
  dir.create("data")
}
if(!dir.exists("plots")) {
  dir.create("plots")
}

if(!file.exists("data/OtwartyWroclaw_rozklad_jazdy_GTFS.zip")) {
  download.file(url = "https://www.gtfsclaw.pl/open-data/87b09b32-f076-4475-8ec9-6020ed1f9ac0/OtwartyWroclaw_rozklad_jazdy_GTFS.zip", destfile = "data/OtwartyWroclaw_rozklad_jazdy_GTFS.zip", method = "wget", extra = "-c")
}
gtfs <- read_gtfs("data/OtwartyWroclaw_rozklad_jazdy_GTFS.zip")
if (as.POSIXct(Sys.Date()) > as.POSIXct.Date(max(gtfs$calendar$end_date))) {
  message("Consider update of the GTFS file.")
}

dbWriteTable(con, "gtfs_stops", gtfs$stops, overwrite = TRUE)
dbWriteTable(con, "gtfs_routes", gtfs$routes, overwrite = TRUE)

#########
library(osmdata)
wrobb <- getbb("Wrocław, Poland")

xml_routes <- opq(wrobb, timeout = 60) |> 
  add_osm_features(features = c("\"route\"= \"bus\"", "\"route\" = \"tram\"", 
                                "\"public_transport\" = \"stop_position\"",
                                "\"public_transport\" = \"platform\"")) |>
  osmdata_xml(filename = "data/xml_routes.xml")

osm_routes <- osmdata_sf(doc = xml_routes)

a <- osm_routes$osm_points |>
  sf::st_drop_geometry()

dbWriteTable(con, "osm_points", a, overwrite = TRUE)
rm(a)

#
# TODO xmls to convert data
#
library(xml2)
relations <- xml2::xml_find_all(xml_routes, xpath = ".//relation")
head(relations)

rel_tags <- data.frame(
  # rel_id = as.integer(),
  # key = as.character(),
  # value = as.character()
)

for (i in 1:length(relations)) {
  print(i)
  rel_id <- as.integer(xml_attr(relations[i], "id"))
  print(paste("rel_id = ",rel_id))
  keys <- xml_find_all(relations[[i]], xpath = ".//tag")
  for(j in 1:length(keys)) {
    rel_tags <- data.frame("rel_id" = rel_id, "key" = xml_attr(keys[j], "k"), "value" = xml_attr(keys[j], "v")) |>
      rbind(rel_tags)
  }
}

rel_tags <- rel_tags |>
  tidyr::pivot_wider(names_from = key, values_from = value)

dbWriteTable(con, "osm_rel_tags", rel_tags, overwrite = TRUE)
rm(rel_tags)
gc()

rel_members <- data.frame()
for (i in 1:length(relations)) {
  print(i)
  rel_id <- as.integer(xml_attr(relations[i], "id"))
  print(paste("rel_id =",rel_id))
  keys <- xml_find_all(relations[[i]], xpath = ".//member")
  for(j in 1:length(keys)) {
    rel_members <- data.frame("rel_id" = rel_id, "type" = xml_attr(keys[j], "type"), "ref" = xml_attr(keys[j], "ref"),
                              "role" = xml_attr(keys[j], "role")) |>
      rbind(rel_members)
  }
}
dbWriteTable(con, "osm_rel_members", rel_members, overwrite = TRUE)
rm(rel_members)
gc()

bbox <- sf::st_bbox(gtfs_as_sf(gtfs)$stops)
polygon <- sf::st_as_sf(sf::st_as_sfc(bbox, crs = 4326)) |>
  sf::st_transform(crs = sf::st_crs(osm_routes$osm_points))

osm_stops <- 
  osm_routes$osm_points |>
  subset(disused.public_transport %in% c("platform", "stop_position") | 
           public_transport %in% c("platform", "stop_position") | 
           highway %in% c("bus_stop", "platform") | railway == "tram_stop") |>
  subset(select = c(osm_id, name, official_name, ref, ref.2, ref.zdik, public_transport, disused.public_transport)) |>
  dplyr::mutate(ref_common = ifelse(is.na(ref) & !is.na(ref.zdik), ref.zdik, ref)) |>
  sf::st_join(polygon, left = FALSE) |>
  dplyr::mutate(long = unlist(purrr::map(geometry,1)), lat = unlist(purrr::map(geometry,2))) |>
  sf::st_drop_geometry()

dbWriteTable(con, "osm_stops", osm_stops, overwrite = TRUE)
rm(osm_stops)
gc()

