###
###  ===   DESTRUCTION DE LA BASE DE DONNEES SUR LE SERVEUR DE L'UQAR
###



library(RPostgreSQL)



### Liste des tables a supprimer (ordre important) -----------------------------

tbnames <- c(
  "argos_localisations",
  "argos_id_colliers",
  "argos_periode_suivi",
  "table_logistique",
  "table_composition_equipe",
  "table_observations_tanieres_pre2009",
  "table_observations_tanieres_jeunes",
  "table_observations_tanieres_adultes",
  "table_visite_tanieres",
  "table_description_tanieres_1trou",
  "table_description_tanieres",
  "table_tanieres_repro",
  "table_echantillons_analyse",
  "table_echantillons_collecte",
  "table_piegeages",
  "table_carcasses",
  "table_contacts_partial",
  "table_contacts",
  "table_cameras_reconyx",
  "table_lemming_grilles",
  "table_lemming_transects",
  "table_identite_tanieres",
  "table_identite_observateurs",
  "table_identite_renards",
  "codes_couleurs_dispo",
  "code_qualite_localisation",
  "code_statut_collier",
  "code_neige",
  "code_visibilite_taniere",
  "code_nombre_feces",
  "code_methode_preservation",
  "code_type_collecte",
  "code_type_piege",
  "code_type_installation",
  "code_type_substrat",
  "code_consistance_femur",
  "code_couleur_femur",
  "code_etat_carcasse",
  "code_methode_trouve",
  "code_statut_taniere",
  "code_type_taniere",
  "code_sort_collier",
  "code_type_collier",
  "code_sort_contact",
  "code_apparence_lait",
  "code_statut_repro",
  "code_type_observation",
  "code_precipitation",
  "code_force_vent",
  "code_couvert_nuageux",
  "code_milieu_environnant",
  "code_orientation",
  "code_comportement",
  "code_type_tissu",
  "code_classe_age_tissu",
  "code_classe_age_carcasse",
  "code_classe_age",
  "code_sexe",
  "code_spp_fox",
  "code_spp_all"
)



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



### Suppression iterative des tables -------------------------------------------

for (i in 1:length(tbnames)){
  dbSendQuery(mydb, paste("DROP TABLE IF EXISTS", tbnames[i]))
}



### Close database connexion ---------------------------------------------------

lapply(dbListConnections(PostgreSQL()), DBI::dbDisconnect)
dbUnloadDriver(dbDriver("PostgreSQL"))
