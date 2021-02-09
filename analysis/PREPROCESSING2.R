library(foreach)
library(parallel)
#install.packages("future.apply")-------------------------

#I. CARGAR CATALOGOS
library(future.apply)
#------------------------------CONFIGURANDO AMBIENTE--------------------------
#Setup working dir
workdir<-"/Volumes/Users/Usuarios/fernandolopez/Dropbox/COVID-19-PROYECTO/COVID-19-PROYECTO"
#workingdir <- dirname(rstudioapi::getActiveDocumentContext()$path)
setwd(workdir)

#-----------------------------CARGAR DATOS-------
#.....1.1. LEER Catalogos (valores nominales escalas variables) del fichero Catalogos_071020.xlsx

xl_catalogos<-"Catalogos_071020.xlsx"
tab_names <- excel_sheets(path = xl_catalogos)
catalogos <- lapply(tab_names, function(x) read_excel(path = xl_catalogos, sheet = x,))
str(catalogos)

#.....1.2  CORREGIR Nombres hojas
fixed_tabs<-lapply(tab_names, function(x) {substr(x,10, nchar(x))})
fixed_tabs[9]<-substr(fixed_tabs[9],4, nchar(fixed_tabs[9]))
print("fixed_tabs->")
fixed_tabs<-unlist(fixed_tabs)
sprintf("%s",fixed_tabs)
names(catalogos) <- fixed_tabs
names(catalogos)

#.....1.3 ESTANDARIZAR nombres de campos
for (c in 1:length(catalogos)) {
  namesc <- names(catalogos[[c]])
  namesc[namesc == "DESCRIPCIÓN"] <- "DESCRIPCION"
  names(catalogos[[c]])<-namesc
}
names(catalogos$CLASIFICACION_FINAL) <- c("CLAVE","DESCRIPCION","DESCRIPCION2")
names(catalogos$ENTIDADES) <- c("CLAVE","DESCRIPCION","ABREVIATURA")
names(catalogos$MUNICIPIOS) <-c("CLAVE","DESCRIPCION","ENTIDAD") 

#II. CARGAR FICHERO DE DATOS DE MÉXICO (PARA AÑADIR NOMBRES DE MUNICIPIOS
data_mexico_file_name <- "data_mexico_parcial.csv"
new_data_mexico <- read.csv(file=data_mexico_file_name, stringsAsFactors = FALSE, header = TRUE)
new_data_mexico_ordered <- new_data_mexico[order(new_data_mexico$deENTIDAD_RES ),]

#III. CREAR COLUMNA deMUNICIPIOS con los nombres de los municipios

rows_by_entity_sorted <- sort(table(new_data_mexico_ordered$deENTIDAD_RES))
#Particionar datos para procesamiento paralelo
entities_a_1 <- names(rows_by_entity_sorted[seq(1, 15, 2)])
entities_b_1 <- names(rows_by_entity_sorted[seq(2, 16, 2)])
entities_a_2 <- names(rows_by_entity_sorted[seq(17, 32, 2)])
entities_b_2 <- names(rows_by_entity_sorted[seq(18, 33, 2)])

#Crear listas para pdocesamiento distribuido en dos computadoras
entities_a <- c(entities_a_1,entities_a_2)
entities_b <- c(entities_b_1,entities_b_2)

#Particionar conjunto de datos en dos listas
new_data_mexico_a <- new_data_mexico[new_data_mexico$deENTIDAD_RES %in% entities_a,]
new_data_mexico_b <- new_data_mexico[new_data_mexico$deENTIDAD_RES %in% entities_b,]

new_data_mexico_a_filename <- "datos_mexico_parcial_1.csv"
new_data_mexico_b_filename <- "datos_mexico_parcial_2.csv"

write.csv(new_data_mexico_a,new_data_mexico_a_filename)
write.csv(new_data_mexico_a,new_data_mexico_a_filename)


res <- foreach(i = , .combine = c, ....) %dopar% {
  # things you want to do
  x <- someFancyFunction()
  
  # the last value will be returned and combined by the .combine function
  x 
}
#data_mexico$deMUNICIPIOS <- NA
#for (irow in 1:nrow(data_mexico)){
#  r <- data_mexico[irow,]
#  idx <- which(data_mexico$MUNICIPIO_RES==r$MUNICIPIO_RES & data_mexico$ENTIDAD_RES ==r$ENTIDAD_RES )
#  irows <- catalogos$MUNICIPIOS$DESCRIPCION[as.integer(catalogos$MUNICIPIOS$CLAVE)==r$MUNICIPIO_RES & as.integer(catalogos$MUNICIPIOS$ENTIDAD)==r$ENTIDAD_RES]
#  if (length(irows)>=1){
#    data_mexico$deMUNICIPIOS[idx] <- irows
#  }
#  else data_mexico$deMUNICIPIOS[idx] <- r$MUNICIPIO_RES
#} 
#---------------Ejecucion en paralelo------------------
clocal <- parallel::makeCluster(detectCores())
clusterExport(cl=clocal,new_data_mexico)  

setDefaultCluster(clocal)
doParallel::registerDoParallel(clocal)

getting_municipalities_names <- function(df,c){
  municipios <- foreach(irow=seq_along(df),.combine="c",.inorder=TRUE) %dopar% {
    r <- df[irow,]
    idx <- which(df$MUNICIPIO_RES==r$MUNICIPIO_RES & df$ENTIDAD_RES ==r$ENTIDAD_RES )
    irows <- c$MUNICIPIOS$DESCRIPCION[as.integer(c$MUNICIPIOS$CLAVE)==r$MUNICIPIO_RES & as.integer(c$MUNICIPIOS$ENTIDAD)==r$ENTIDAD_RES]
    if (length(irows)>=1){
        irows
    }
    else r$MUNICIPIO_RES
  }
}
deMUNICIPIOS <- foreach(e=new_data_mexico$ENTIDAD_NAC, .combine="c" )
deMUNICIPIOS <-future_mclapply(new_data_mexico,new_data_mexico$ENTIDAD_NAC,getting_municipalities_names,catalogos)
stopCluster(clocal)
#grouped_by_entity_nac <- split(new_data_mexico,f=new_data_mexico$ENTIDAD_NAC)
#test <- foreach(e=unique_entities_nac
#new_data_mexico$deMUNICIPIOS <- foreach(i = nrow(new_data_mexico)) %:% when()
# Cluster setup
#clNode <- list(host="localhost")
#localCl <- makeSOCKcluster(rep(clNode, 2))
#clusterSetupRNG(localCl, type="RNGstream", seed=sample(0:9,6,replace=TRUE))
#clusterExport(localCl, lisdf$ENTIDAD_RES ==r$ENTIDAD_RES)
# Break into list
#datagroup <- split(data, factor(data$group))
#output <- clusterApply(localCl, datagroup, function(x){ x$a*x$b })
# Put back and check
#data$output <- do.call(c, output)
#data$check <- data$a*data$b
#all(data$output==data$check)
# Stop cluster
