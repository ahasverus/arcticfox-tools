###
###  ===   DESTRUCTION DE LA BASE DE DONNEES SUR LE SERVEUR DE L'UQAR
###



### Installation (si necessaire) et chargement du package requis ---------------

# install.packages("RPostgreSQL")
library(RPostgreSQL)



### Database connexion ---------------------------------------------------------

drv  <- dbDriver("PostgreSQL")

mydb <- dbConnect(
  drv      = drv,
  host     = "Veuillez ecrire ici l'hote du serveur",
  port     = 5432,
  user     = "Veuillez ecrire ici le nom d'utilisateur",
  password = "Veuillez ecrire ici le mot de passe",
  dbname   = "Veuillez ecrire ici le nom de la base de données"
)



###  ===   UPDATE < COORDINATES (LEMMINGS) > -----------------------------------

# Get records ids for which Coordinates are NULL
rows <- dbGetQuery(
  mydb,
  paste0(
    "SELECT annee,no_transect ",
    "FROM table_lemming_transects ",
    "WHERE utm_est IS NULL"
  )
)

# If there is at least one record
if (nrow(rows) > 0){

  for (i in 1:nrow(rows)){

    # Prepare SQL query to identify these transects in previous years
    clause <- paste0(
      "WHERE no_transect = '", rows[i, "no_transect"], "' ",
      "AND annee < ", rows[i, "annee"]
    )

    # Get all records for the transect (previous years)
    tmp <- dbGetQuery(
      mydb,
      paste0(
        "SELECT annee,utm_est,utm_nord ",
        "FROM table_lemming_transects ",
        clause
      )
    )

    # If there is one record (previous years)
  	if (nrow(tmp) > 0){

      # Remove rows without coordinates
      tmp <- tmp[!is.na(tmp[ , "utm_est"]), ]

      # If there is still at least one record (previous years + coordinates)
      if (nrow(tmp) > 0){

        # Get coordinates of the last transect description
        pos <- which(tmp[ , "annee"] == max(tmp[ , "annee"]))

        # Prepare SQL query to send UPDATE
  			clause <- paste0(
          "WHERE no_transect = '", rows[i, "no_transect"], "' ",
          "AND annee = ", rows[i, 'annee']
        )

        # Update UTM East
        dbSendQuery(
          mydb,
          paste0(
            "UPDATE table_lemming_transects ",
            "SET utm_est = '", tmp[pos, "utm_est"], "' ",
            clause
          )
        )

        # Update UTM North
  			dbSendQuery(
          mydb,
          paste0(
            "UPDATE table_lemming_transects ",
            "SET utm_nord = '", tmp[pos, "utm_nord"], "' ",
            clause
          )
        )
  		}
  	}
  }
}



###  ===   UPDATE < HABITAT (LEMMINGS) > ---------------------------------------

# Get records ids for which Habitat are NULL
rows <- dbGetQuery(
  mydb,
  paste0(
    "SELECT annee,no_transect ",
    "FROM table_lemming_transects ",
    "WHERE type_habitat IS NULL"
  )
)

# If there is at least one record
if (nrow(rows) > 0){

  for (i in 1:nrow(rows)){

    # Prepare SQL query to identify these transects in previous years
  	clause <- paste0(
      "WHERE no_transect = '", rows[i, "no_transect"], "' ",
      "AND annee != ", rows[i, "annee"]
    )

    # Get all records for the transect (previous years)
    tmp <- dbGetQuery(
      mydb,
      paste0(
        "SELECT annee,type_habitat ",
        "FROM table_lemming_transects ",
        clause
      )
    )

    # If there is one record (previous years)
  	if (nrow(tmp) > 0){

      # Frequency table of all habitats for this transect
  		pos <- table(tmp[ , "type_habitat"])

      # Get the most frequent habitat
  		hab <- names(pos[which(pos == max(pos))])

      # Prepare SQL query to send UPDATE
      clause <- paste0(
        "WHERE no_transect = '", rows[i, "no_transect"], "' ",
        "AND annee = ", rows[i, "annee"]
      )

      # Update Habitat
      dbSendQuery(
        mydb,
        paste0(
          "UPDATE table_lemming_transects ",
          "SET type_habitat = '", hab, "' ",
          clause
        )
      )
  	}
  }
}



###  ===   UPDATE < NO AUTRES CAMERAS (CAMERAS) > ------------------------------

# Get all records
rows <- dbGetQuery(
  mydb,
  paste0(
    "SELECT date,no_taniere,no_camera ",
    "FROM table_cameras_reconyx"
  )
)

# Extract year
rows[ , "year"] <- substr(rows[ , "date"], 1, 4)

for (i in 1:nrow(rows)){

  # Get all rows for the year and the den
  pos <- which(
    rows[ , "year"]       == rows[i, "year"]      &
    rows[ , "no_taniere"] == rows[i, "no_taniere"]
  )

  # Get unique cameras for the year and the den
  cam <- unique(rows[pos, "no_camera"])

  # Remove the camera i
  cam <- cam[-which(cam == rows[i, "no_camera"])]

  # If there is another cameras
	if (length(cam) > 0){

    # Get camera 2 id
		other <- cam[1]

    # If there is more than two cameras
		if (length(cam) > 1){

      # Append
			for (j in 2:length(cam)){
				other <- paste0(other, ' ; ', cam[j])
      }
		}

    # Prepare SQL query to send UPDATE
		clause <- paste0(
      "WHERE date = '", rows[i, "date"], "' ",
      "AND no_taniere = '", rows[i, "no_taniere"], "' ",
      "AND no_camera = '", rows[i, "no_camera"], "'"
    )

    # Update
    dbSendQuery(
      mydb,
      paste0(
        "UPDATE table_cameras_reconyx ",
        "SET no_autre_camera = '", other, "' ",
        clause
      )
    )
	}
}



###  ===   UPDATE < ID et SEXE (CARCASSES) > -----------------------------------

# Get code couleur of all dead foxes
ids <- dbGetQuery(
  mydb,
  paste0(
    "SELECT code_couleur ",
    "FROM table_carcasses"
  )
)

# Sort by code couleur
ids <- sort(unique(ids[!is.na(ids[ , 1]), 1]))

for (i in 1:length(ids)){

  # Prepare SQL query to get ID and sex
	clause <- paste0(
    "WHERE code_couleur = '", ids[i], "'"
  )

  # Get ID and sex
  infos <- dbGetQuery(
    mydb,
    paste0(
      "SELECT id_renard,sexe ",
      "FROM table_identite_renards ",
      clause
    )
  )

  # Update ID
  dbSendQuery(
    mydb,
    paste0(
      "UPDATE table_carcasses ",
      "SET id_renard = '", infos[1, "id_renard"], "' ",
      clause
    )
  )

  # Update Sex
  dbSendQuery(
    mydb,
    paste0(
      "UPDATE table_carcasses ",
      "SET sexe = '", infos[1, "sexe"], "' ",
      clause
    )
  )
}



###  ===   UPDATE < ID et NOM (CONTACTS) > -------------------------------------

# Get code couleur of all foxes with a contact
ids <- dbGetQuery(
  mydb,
  paste0(
    "SELECT code_couleur ",
    "FROM table_contacts"
  )
)

# Sort by code couleur and get unique id
ids <- sort(unique(ids[!is.na(ids[ , 1]), 1]))

for (i in 1:length(ids)){

  # Prepare SQL query to get ID and Name
  clause <- paste0(
    "WHERE code_couleur = '", ids[i], "'"
  )

  # Get ID and Name
  infos <- dbGetQuery(
    mydb,
    paste0(
      "SELECT id_renard,nom ",
      "FROM table_identite_renards ",
      clause
    )
  )

  # Update ID
  dbSendQuery(
    mydb,
    paste0(
      "UPDATE table_contacts ",
      "SET id_renard = '", infos[1, "id_renard"], "' ",
      clause
    )
  )

  # Update Name (if There is one)
  if (!is.na(infos[1, "nom"])){

    infos[1, "nom"] <- gsub("'", "''", infos[1, "nom"], perl = TRUE)

    dbSendQuery(
      mydb,
      paste0(
        "UPDATE table_contacts ",
        "SET nom = '", infos[1, "nom"], "' ",
        clause
      )
    )
	}
}



###  ===   UPDATE < COORDINATES (CONTACTS) > -----------------------------------

# Get code couleur of all dead foxes
ids <- dbGetQuery(
  mydb,
  paste0(
    "SELECT no_taniere ",
    "FROM table_contacts"
  )
)

ids <- sort(unique(ids[!is.na(ids[ , 1]), 1]))

for (i in 1:length(ids)){

  # Prepare SQL query
  clause <- paste0(
    "WHERE no_taniere = '", ids[i], "'"
  )

  infos <- dbGetQuery(
    mydb,
    paste0(
      "SELECT utm_est,utm_nord,nad ",
      "FROM table_identite_tanieres ",
      clause
    )
  )

  dbSendQuery(
    mydb,
    paste0(
      "UPDATE table_contacts ",
      "SET utm_nord = '", infos[1, "utm_nord"], "' ",
      clause
    )
  )

	dbSendQuery(
    mydb,
    paste0(
      "UPDATE table_contacts ",
      "SET utm_est = '", infos[1, "utm_est"], "' ",
      clause
    )
  )

	dbSendQuery(
    mydb,
    paste0(
      "UPDATE table_contacts ",
      "SET nad = '", infos[1, "nad"], "' ",
      clause
    )
  )
}



###  ===   UPDATE < ID et NOM (ECHANTILLONS) > ---------------------------------

ids <- dbGetQuery(
  mydb,
  paste0(
    "SELECT code_couleur ",
    "FROM table_echantillons_collecte"
  )
)

ids <- sort(unique(ids[!is.na(ids[ , 1]), 1]))

for (i in 1:length(ids)){

	clause <- paste0(
    "WHERE code_couleur = '", ids[i], "'"
  )

  infos <- dbGetQuery(
    mydb,
    paste0(
      "SELECT id_renard,sexe ",
      "FROM table_identite_renards ",
      clause
    )
  )

  dbSendQuery(
    mydb,
    paste0(
      "UPDATE table_echantillons_collecte ",
      "SET id_renard = '", infos[1, "id_renard"], "' ",
      clause
    )
  )

  dbSendQuery(
    mydb,
    paste0(
      "UPDATE table_echantillons_collecte ",
      "SET sexe = '", infos[1, "sexe"], "' ",
      clause
    )
  )
}



###  ===   Update < COORDINATES (ECHANTILLONS) > -------------------------------

ids <- dbGetQuery(
  mydb, paste0(
    "SELECT no_taniere ",
    "FROM table_echantillons_collecte"
  )
)

ids <- sort(unique(ids[!is.na(ids[ , 1]), 1]))

for (i in 1:length(ids)){

	clause <- paste0(
    "WHERE no_taniere = '", ids[i], "'"
  )

  infos <- dbGetQuery(
    mydb,
    paste0(
      "SELECT utm_est,utm_nord,nad ",
      "FROM table_identite_tanieres ",
      clause
    )
  )

  dbSendQuery(mydb,
    paste0(
      "UPDATE table_echantillons_collecte ",
      "SET utm_nord = '", infos[1, "utm_nord"], "' ",
      clause
    )
  )

  dbSendQuery(
    mydb,
    paste0(
      "UPDATE table_echantillons_collecte ",
      "SET utm_est = '", infos[1, "utm_est"], "' ",
      clause
    )
  )

	dbSendQuery(
    mydb,
    paste0(
      "UPDATE table_echantillons_collecte ",
      "SET nad = '", infos[1, "nad"], "' ",
      clause
    )
  )
}



###  ===   Update < DEAD ANIMALS > ---------------------------------------------

car <- dbGetQuery(
  mydb,
  paste0(
    "SELECT code_couleur ",
    "FROM table_carcasses"
  )
)

car <- car[which(!is.na(car[ , 1])), 1]

con <- dbGetQuery(
  mydb,
  paste0(
    "SELECT code_couleur ",
    "FROM table_contacts ",
    "WHERE type_observation = 'CAR'"
  )
)

arg <- dbGetQuery(
  mydb,
  paste0(
    "SELECT ptt_user ",
    "FROM argos_periode_suivi ",
    "WHERE statut = 'D'"
  )
)

arg <- arg[ , 1]

ids <- dbGetQuery(
  mydb,
  paste0(
    "SELECT code_couleur, ptt_user_1, ptt_user_2, ptt_user_3, ptt_user_4, ptt_user_5 ",
    "FROM argos_id_colliers"
  )
)

ptt <- NULL

for (i in 2:ncol(ids)){

	ptt <- c(ptt, ids[which(ids[ , i] %in% arg), 1])
}

co <- dbGetQuery(
  mydb,
  "SELECT * FROM table_contacts"
)

ids <- unique(co[ , "code_couleur"])

old <- NULL

for (i in 1:length(ids)){

  pos <- which(co[ , "code_couleur"] == ids[i])
  sop <- which.min(co[pos, "date"])[1]
  if ((as.numeric(format(Sys.Date(), "%Y")) - as.numeric(substr(co[pos[sop], "date"], 1, 4))) > 14){
    old <- c(old, ids[i])
  }
}

car <- unique(c(car, con, ptt, old))

for (i in 1:length(car)){

  clause <- paste0("WHERE code_couleur = '", car[i], "'")

  dbSendQuery(
    mydb,
    paste0(
      "UPDATE table_identite_renards ",
      "SET statut = 'DEAD' ",
      clause
    )
  )
}



###  ===   Update < CODES COULEURS DISPONIBLES > -------------------------------

ids <- dbGetQuery(
  mydb,
  paste0("SELECT code_couleur FROM table_identite_renards"
  )
)

ids <- unique(substr(ids[ , 1], 1, 4))

cc <- dbGetQuery(
  mydb,
  "SELECT * FROM codes_couleurs_dispo"
)


for (i in 1:length(ids)){

  clause <- paste0(
    "WHERE code_couleur = '", ids[i], "'"
  )

  dbSendQuery(mydb,
    paste0(
      "DELETE FROM codes_couleurs_dispo ", clause
    )
  )
}



### Close database connexion ---------------------------------------------------

lapply(dbListConnections(PostgreSQL()), DBI::dbDisconnect)
dbUnloadDriver(dbDriver("PostgreSQL"))
