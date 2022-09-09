# devtools::install_github("r-transit/tidytransit")
# install.packages('duckdb', repos=c('https://duckdb.r-universe.dev', 'https://cloud.r-project.org'))

source("data_preparation.R")

# dbGetQuery(con, "SHOW TABLES")
#              name
# 1     gtfs_routes
# 2      gtfs_stops
# 3      osm_points
# 4 osm_rel_members
# 5    osm_rel_tags
# 6       osm_stops

########### data preparation end

odl <- function(x1, y1, x2, y2) {
  if(is.na(x1) | is.na(y1) | is.na(x2) | is.na(y2)) {
    return(NA)
  }
  dist <- sf::st_distance(
    sf::st_transform(sf::st_sfc(sf::st_point(c(x1, y1)), crs = 4326), crs = 2180),
    sf::st_transform(sf::st_sfc(sf::st_point(c(x2, y2)), crs = 4326), crs = 2180))
  dist <- as.numeric(dist)
  return(dist)
}

# odl(16.90589, 51.19808, 16.90579, 51.19809)

# ref vs zdik.ref

dbGetQuery(con, paste("SELECT * FROM osm_stops WHERE \"disused.public_transport\" IS NOT NULL"))
dbGetQuery(con, paste("SELECT COUNT(*) FROM osm_stops WHERE ref IS NOT NULL"))[[1]]
dbGetQuery(con, paste("SELECT COUNT(*) FROM osm_stops WHERE ref IS NULL AND \"ref.zdik\" IS NOT NULL"))[[1]]

check_osm_stop_positions <- 
  dbGetQuery(con, paste("SELECT * FROM gtfs_stops",
                        "JOIN osm_stops ON (gtfs_stops.stop_code = osm_stops.ref_common)",
                        "WHERE osm_stops.public_transport = 'stop_position'",
                        "OR osm_stops.\"disused.public_transport\" = 'stop_position'",
                        ""))

# adding column with distance [in meters] between gtfs stops and osm stops
check_osm_stop_positions <- 
  check_osm_stop_positions |>
    dplyr::rowwise() |>
    dplyr::mutate(distance = odl(stop_lon, stop_lat, long, lat)) |>
    dplyr::ungroup()

dbWriteTable(con, "results_check_osm_stop_positions", check_osm_stop_positions, overwrite = TRUE)

hist(check_osm_stop_positions$distance, 
     breaks = c(0,10,20,30,40,50,60,70,80,90,100,200,1000,3000), 
     xlab = "Distance [m]",
     main = "GTFS stop vs. public_transport = stop_position",
     xlim = c(0, 100), freq = TRUE, labels = TRUE)

check_osm_stop_positions |>
  dplyr::arrange(desc(distance)) |>
  subset(distance >= 20) |>
  subset(select = c(stop_id, stop_code, stop_name, osm_id, name, ref, ref.zdik, distance))


check_osm_platform <- 
  dbGetQuery(con, paste("SELECT * FROM gtfs_stops",
                        "JOIN osm_stops ON (gtfs_stops.stop_code = osm_stops.ref_common)",
                        "WHERE osm_stops.public_transport = 'platform'",
                        "OR osm_stops.\"disused.public_transport\" = 'platform'",
                        ""))

check_osm_platform <- check_osm_platform |>
  dplyr::rowwise() |>
  dplyr::mutate(distance = odl(stop_lon, stop_lat, long, lat)) |>
  dplyr::ungroup()

dbWriteTable(con, "results_check_osm_platform", check_osm_platform, overwrite = TRUE)

hist(check_osm_platform$distance, 
     breaks = c(0,10,20,30,40,50,60,70,80,90,100,200,1000,3000), 
     xlab = "Distance [m]",
     main = "GTFS stop vs. public_transport = platform",
     xlim = c(0, 100), freq = TRUE, labels = TRUE)

check_osm_platform |>
  dplyr::arrange(desc(distance)) |>
  subset(distance >= 20) |>
  subset(select = c(stop_id, stop_code, stop_name, osm_id, name, ref, ref.zdik, distance))


dbGetQuery(con, paste("SELECT * FROM gtfs_stops WHERE stop_code = '425903'"))

dbGetQuery(con, paste(
  "SELECT * FROM gtfs_stops WHERE stop_code NOT IN",
  "(SELECT distinct(ref) FROM osm_stops WHERE public_transport IN ('platform', 'stop_position') AND ref IS NOT NULL)")) |> head()
  

# TODO dokończyć sprawdzanie linii
osm_routes$osm_points |>
  subset(ref == '18333' | name == 'Las Ratyński') |>
  subset(select = c(osm_id, name, official_name, ref, public_transport, disused.public_transport))

dbGetQuery(con, paste("SELECT COUNT(*) FROM osm_stops",
  "WHERE public_transport in ('platform', 'stop_position')",
  "AND ref IS NULL AND \"ref.zdik\" IS NOT NULL"))

nrow(check_osm_stop_positions)

top_variants <- data.frame()
for(route in unique(gtfs$routes$route_id)) {
  print(route)
  trips <- gtfs$trips |>
    subset(route_id == route) |>
    dplyr::group_by(route_id, variant_id, direction_id) |>
    dplyr::count()
  for (direction in unique(trips$direction_id)) {
    top_variants <- 
      trips |>
      subset(direction_id == direction) |>
      dplyr::arrange(desc(n)) |>
      head(1) |>
      dplyr::left_join(gtfs$trips, by="variant_id") |>
      head(1) |>
      dplyr::select(c("route_id" = "route_id.x", "variant_id", "direction_id" = "direction_id.x", "n", "trip_id")) |>
      rbind(top_variants)
  }
}

gtfs_routes <- top_variants |>
  dplyr::left_join(gtfs$stop_times, by = "trip_id") |>
  dplyr::left_join(gtfs$stops, by="stop_id")

head(gtfs_routes)
dbWriteTable(con, "gtfs_top_routes", gtfs_routes, overwrite = TRUE)
res = dbGetQuery(con, "SELECT * FROM gtfs_top_routes WHERE route_id = '109' AND stop_sequence = 0 AND lower(stop_name) = 'pl. solidarności'")
print(res)
rm(gtfs_routes)


osm_spr <- rel_members |>
  subset(rel_id == 2395221 & type == 'node' & role %in% c("stop_entry_only", "stop", "stop_exit_only")) |>
  dplyr::left_join(osm_routes$osm_points, by = c("ref" = "osm_id")) |>
  subset(select = c(rel_id, type.x, ref, role, name, official_name, highway, public_transport, disused.public_transport, ref.y))

osm_spr <- dbGetQuery(con, paste(
  "SELECT rel_id, rel_members.type, role, osm_id, name, official_name,",
          "public_transport, \"disused.public_transport\", osm_points.ref FROM rel_members",
  "JOIN osm_points ON (rel_members.ref = osm_points.osm_id)",
  "WHERE rel_members.rel_id = 2395221",
      "AND rel_members.type = 'node' AND rel_members.role IN ('stop_entry_only', 'stop', 'stop_exit_only')"))
print(osm_spr)

points <- 
  osm_routes$osm_points |>
  subset((disused.public_transport == "stop_position" | public_transport == "stop_position")) |>
  subset(select = c(osm_id, name, official_name, ref, public_transport, disused.public_transport)) |>
  sf::st_drop_geometry() 

b <- dbGetQuery(con, paste(
  "SELECT stop_sequence, stop_code, stop_name FROM gtfs_routes",
  "WHERE route_id = '109' AND direction_id = 1",
  "ORDER BY stop_sequence")) |>
  dplyr::mutate(osm_id = ifelse(stop_code %in% osm_spr$ref, osm_spr$osm_id[match(stop_code, osm_spr$ref)], NA)) |>
  dplyr::mutate(osm_name = ifelse(stop_code %in% osm_spr$ref, osm_spr$name[match(stop_code, osm_spr$ref)], NA))

  # gtfs_routes |>
  # subset(route_id == '109' & direction_id == 1) |>
  # dplyr::arrange(stop_sequence) |>
  # subset(select = c("stop_sequence", "stop_code", "stop_name")) |>
  # dplyr::mutate(c = ifelse(b$stop_code %in% osm_spr$ref.y, points$osm_id[match(b$stop_code, points$ref)], NA))


dbGetQuery(con, "SELECT DISTINCT(route_id, direction_id) FROM gtfs_routes LIMIT 6")

unique(osmroutes$osm_points$highway)
osmroutes$osm_points |>
  subset(osm_id == 1895871902) |>
  subset(select = c(osm_id, name, highway, public_transport, ref))

osmroutes$osm_multilines |>
  subset(ref == '109')

str(osmroutes)

osmroutes$osm_points |>
  subset(!is.na(public_transport) & ref == '12153') |>
  subset(select = c(osm_id, name, ref))

