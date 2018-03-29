###
###  ===   MISE A JOUR ANNEXES PROTOCOLE RENARDS
###



library(RPostgreSQL)



### Directory to export annexes ------------------------------------------------

outdir <- "C:/Users/admin/Desktop/"



### Database connexion ---------------------------------------------------------

drv <- dbDriver("PostgreSQL")

mydb <- dbConnect(
  drv      = drv,
  host     = "Veuillez ecrire ici l'hote du serveur",
  port     = 5432,
  user     = "Veuillez ecrire ici le nom d'utilisateur",
  password = "Veuillez ecrire ici le mot de passe",
  dbname   = "Veuillez ecrire ici le nom de la base de données"
)



### Annexe 03 - Dens coordinates -----------------------------------------------

# Extract data
den <- dbGetQuery(
  mydb,
  "SELECT * FROM table_identite_tanieres"
)

# Subset dens
den <- den[which(den[ , "utm_est"] != 999999 & den[ , "suivi"] == "Oui"), ]

# Order den by name
den <- den[order(as.character(den[ , "no_taniere"])), ]

# Export data
write.table(
  x         = den,
  file      = paste0(outdir, "/annexe03.txt"),
  sep       = "\t",
  row.names = FALSE
)



### Annexe 08 - Arctic foxes identities ----------------------------------------

# Extract data
cc <- dbGetQuery(mydb, "SELECT * FROM codes_couleurs_dispo")
co <- dbGetQuery(mydb, "SELECT * FROM table_contacts")
id <- dbGetQuery(mydb, "SELECT * FROM table_identite_renards")

# Select arctic foxes
ra <- id[id[ , "espece"] == "RA", ]

# Select columns
ra <- ra[ , c("id_renard", "code_couleur", "no_tag_gauche", "no_tag_droite", "sexe", "nom", "statut")]

# Extract date, code couleur and age in contacts
ra_co <- co[which(co[ , "code_couleur"] %in% ra[ , "code_couleur"]), c("date", "code_couleur", "age")]

age <- ccol <- year <- NULL

for (i in 1:length(unique(ra[ , "code_couleur"]))) {

  pos  <- which(ra_co[ , "code_couleur"] == unique(ra[ , "code_couleur"])[i])
  sop  <- which.min(ra_co[pos, "date"])[1]

  age  <- c(age, ra_co[pos[sop], "age"])
  year <- c(year, substr(ra_co[pos[sop], "date"], 1, 4))
  ccol <- c(ccol, ra_co[pos[sop], "code_couleur"])
}

ra_co <- data.frame(code_couleur = ccol, age, year)

# Add date, code couleur and age to fox identities
ra <- merge(ra, ra_co, by = "code_couleur", all = TRUE)

# Sort by fox ids
ra <- ra[order(as.character(ra[ , "code_couleur"])), ]

# Select columns
ra <- ra[ , c(2, 1, 3, 4, 5, 7, 9, 8, 6)]
colnames(ra) <- c(
  "ID", "Code", "Tag_Gauche", "Tag_Droite", "Sexe", "Statut", "1e_capture",
  "Age", "Notes"
)

# Replace NA by ""
for (i in 1:ncol(ra)) {
  pos <- which(is.na(ra[ , i]))
  if (length(pos) > 0) {
    ra[pos, i] <- ""
  }
}

# Clean contents
ra[ , "Statut"] <- gsub("DEAD", "R.I.P.", ra[ , "Statut"])
ra[ , "Age"]    <- gsub("A", "Ad.", ra[ , "Age"])
ra[ , "Age"]    <- gsub("J", "Sub.", ra[ , "Age"])

# Export data
write.table(
  x         = ra,
  file      = paste0(outdir, "/annexe08.txt"),
  sep       = "\t",
  row.names = FALSE
)



### Annexe 09 - Red foxes identities -------------------------------------------

# Extract data
cc <- dbGetQuery(mydb, "SELECT * FROM codes_couleurs_dispo")
co <- dbGetQuery(mydb, "SELECT * FROM table_contacts")
id <- dbGetQuery(mydb, "SELECT * FROM table_identite_renards")

# Select red foxes
rr <- id[id[ , "espece"] == "RR", ]

# Select columns
rr <- rr[ , c("id_renard", "code_couleur", "no_tag_gauche", "no_tag_droite", "sexe", "nom", "statut")]

# Extract date, code couleur and age in contacts
rr_co <- co[which(co[ , "code_couleur"] %in% rr[ , "code_couleur"]), c("date", "code_couleur", "age")]

age <- ccol <- year <- NULL

for (i in 1:length(unique(rr[ , "code_couleur"]))) {

  pos  <- which(rr_co[ , "code_couleur"] == unique(rr[ , "code_couleur"])[i])
  sop  <- which.min(rr_co[pos, "date"])[1]

  age  <- c(age, rr_co[pos[sop], "age"])
  year <- c(year, substr(rr_co[pos[sop], "date"], 1, 4))
  ccol <- c(ccol, rr_co[pos[sop], "code_couleur"])
}

rr_co <- data.frame(code_couleur = ccol, age, year)

# Add date, code couleur and age to fox identities
rr <- merge(rr, rr_co, by = "code_couleur", all = TRUE)

# Sort by fox ids
rr <- rr[order(as.numeric(as.character(rr[ , "id_renard"]))), ]

# Select columns
rr <- rr[ , c(2, 1, 3, 4, 5, 7, 9, 8, 6)]
colnames(rr) <- c(
  "ID", "Code", "Tag_Gauche", "Tag_Droite", "Sexe", "Statut", "1e_capture",
  "Age", "Notes"
)

# Replace NA by ""
for (i in 1:ncol(rr)) {
  pos <- which(is.na(rr[ , i]))
  if (length(pos) > 0) {
    rr[pos, i] <- ""
  }
}

# Clean contents
rr[ , "Statut"] <- gsub("DEAD", "R.I.P.", rr[ , "Statut"])
rr[ , "Age"]    <- gsub("A", "Ad.", rr[ , "Age"])
rr[ , "Age"]    <- gsub("J", "Sub.", rr[ , "Age"])

# Export data
write.table(
  x         = rr,
  file      = paste0(outdir, "/annexe09.txt"),
  sep       = "\t",
  row.names = FALSE
)



### Annexe A10 - Available code couleurs ---------------------------------------

# Get data
cc <- dbGetQuery(mydb, "SELECT * FROM codes_couleurs_dispo")

# Replace NA by ""
for (i in 1:ncol(cc)) {
  pos <- which(is.na(cc[ , i]))
  if (length(pos) > 0) {
    cc[pos, i] <- ""
  }
}

# Sort code couleurs
cc <- cc[order(as.character(cc$code_couleur)), ]

# Export data
write.table(
  x         = cc,
  file      = paste0(outdir, "/annexe10.txt"),
  sep       = "\t",
  row.names = FALSE
)



### Close database connexion ---------------------------------------------------

lapply(dbListConnections(PostgreSQL()), DBI::dbDisconnect)
dbUnloadDriver(dbDriver("PostgreSQL"))
