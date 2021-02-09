#library(foreach)
#library(parallel)

install.packages("readxl")
library(readxl)
#------------------------------I. CONFIGURANDO AMBIENTE--------------------------
#Setup working dir
workdir<-"/Volumes/Users/Usuarios/fernandolopez/Dropbox/COVID-19-PROYECTO/COVID-19-PROYECTO"
#workingdir <- dirname(rstudioapi::getActiveDocumentContext()$path)
setwd(workdir)

#-----------------------------II. CARGAR DATOS-------
#.....2.1. LEER Catalogos (valores nominales escalas variables) del fichero fixed_catalogos.xlsx

xl_catalogos<-"fixed_catalogos.xlsx"
#new_tab_names <- excel_sheets(path = xl_catalogos)
new_catalogos <- read_excel(path = xl_catalogos, sheet = "MUNICIPIOS",)
#new_catalogos <- lapply(tab_names, function(x) read_excel(path = xl_catalogos, sheet = x,))
#names(new_catalogos) <- new_tab_names
#new_catalogos <- new_catalogos$MUNICIPIOS
new_catalogos$CLAVE <- as.integer(new_catalogos$CLAVE)
new_catalogos$ENTIDAD <- as.integer(new_catalogos$ENTIDAD)
str(new_catalogos)

#.....2.2 CARGAR FICHERO DE DATOS DE MÉXICO (PARA AÑADIR NOMBRES DE MUNICIPIOS
data_mexico_file_name <- "datos_mexico_parcial_b.csv"
new_data_mexico_b <- read.csv(file=data_mexico_file_name, stringsAsFactors = FALSE, header = TRUE)


#---------------------------III.CREAR COLUMNA DE DESCRIPTORES DE MUNICIPIOS ------------
entidades_municipios_unicas <- unique(new_data_mexico_b[,c("MUNICIPIO_RES","ENTIDAD_RES","deMUNICIPIOS")])
#....3.1 Crear nueva columna para los desciptores de los municipios
new_data_mexico_b$deMUNICIPIOS <- NA
for(irow in 1:nrow(new_data_mexico_b)) {
  mr <- new_data_mexico_b$MUNICIPIO_RES[irow]
  er <- new_data_mexico_b$ENTIDAD_RES[irow]
  if (nrow(new_catalogos$MUNICIPIOS[new_catalogos$MUNICIPIOS$CLAVE == mr & new_catalogos$MUNICIPIOS$ENTIDAD == er,]) >0)
    new_data_mexico_b$deMUNICIPIOS[irow] <- new_catalogos$DESCRIPCION[new_catalogos$CLAVE==mr & new_catalogos$ENTIDAD == er]
}
suma<-0
for (n in 1:length(entidades_por_municipios)) {
  if (entidades_por_municipios[n] <= nrow(catalogos$MUNICIPIOS[catalogos$MUNICIPIOS$CLAVE==as.integer(names(entidades_por_municipios[n])),])) { 
      suma <- suma+1 
  }
}

sume <- 0
for (i in 1:nrow(entidades_municipios_unicas)) {
  if (!is.na(new_data_mexico_b$deMUNICIPIOS)){
    for (j in 1:nrow(catalogos$MUNICIPIOS)){
      if (nrow(catalogos$MUNICIPIOS[as.integer(catalogos$MUNICIPIOS$CLAVE[j]) == entidades_municipios_unicas[i,"MUNICIPIO_RES"] & as.integer(catalogos$MUNICIPIOS$ENTIDAD[j])==entidades_municipios_unicas[i,"ENTIDAD_RES"],])>0){
        sume <- sume+1
      }
    }
    
  }
}

#---------------------------IV. SALVAR FICHERO PREPROCESADO CON LA COLUMNA AÑADIDA deMUNICIPIOS
actual_data_file_name <- "datos_mexico_b.csv"
write.csv(data_mexico,file=actual_data_file_name)


#....3.2 Crear y configurar cluster para la ejecución "multicore"
#clocal <- parallel::makeCluster(detectCores())
#clusterExport(cl=clocal,new_data_mexico_b)  
#clusterExport(cl=clocal,catalogos)

#setDefaultCluster(clocal)
#doParallel::registerDoParallel(clocal)

#....3.3 Asignación de nombres de municipios en paralelo a la columna "deMUNICIPIOS"
#irow <- seq_along(new_data_mexico_b)
#new_data_mexico_b$deMUNICIPIOS <- foreach(irow = seq_along(new_data_mexico_b),.combine="c",.inorder=TRUE)  %:% when(length(new_catalogos$CLAVE==new_data_mexico_b$MUNICIPIO_RES[irow] & new_catalogos$ENTIDAD == new_data_mexico_b$ENTIDAD_RES[irow]) >0) %dopar% new_catalogos$DESCRIPCION[new_catalogos$CLAVE==new_data_mexico_b$MUNICIPIO_RES[irow] & new_catalogos$ENTIDAD == new_data_mexico_b$ENTIDAD_RES[irow]]
#stopCluster(clocal)

#---------------------------IV. SALVAR FICHERO PREPROCESADO CON LA COLUMNA AÑADIDA deMUNICIPIOS
#actual_data_file_name <- "datos_mexico_b.csv"
#write.csv(data_mexico,file=actual_data_file_name)


