library(foreach)
library(parallel)
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
new_data_mexico_a <- read.csv(file=data_mexico_file_name, stringsAsFactors = FALSE, header = TRUE)


#---------------------------III.CREAR COLUMNA DE DESCRIPTORES DE MUNICIPIOS ------------

#....3.1 Crear nueva columna para los desciptores de los municipios
new_data_mexico_a$deMUNICIPIOS <- NA

#....3.2 Crear y configurar cluster para la ejecución "multicore"
#clocal <- parallel::makeCluster(detectCores())
#clusterExport(cl=clocal,new_data_mexico_b)  
#clusterExport(cl=clocal,catalogos)

#setDefaultCluster(clocal)
#doParallel::registerDoParallel(clocal)

#....3.3 Asignación de nombres de municipios en paralelo a la columna "deMUNICIPIOS"
irow <- seq_along(new_data_mexico_a)
new_data_mexico_a$deMUNICIPIOS <- foreach(irow = seq_along(new_data_mexico_a),.combine="c",.inorder=TRUE)  %:% when(length(new_catalogos$CLAVE==new_data_mexico_a$MUNICIPIO_RES[irow] & new_catalogos$ENTIDAD == new_data_mexico_a$ENTIDAD_RES[irow]) >0) %dopar% new_catalogos$DESCRIPCION[new_catalogos$CLAVE==new_data_mexico_a$MUNICIPIO_RES[irow] & new_catalogos$ENTIDAD == new_data_mexico_a$ENTIDAD_RES[irow]]
stopCluster(clocal)

#---------------------------IV. SALVAR FICHERO PREPROCESADO CON LA COLUMNA AÑADIDA deMUNICIPIOS
actual_data_file_name <- "datos_mexico_a.csv"
write.csv(data_mexico,file=actual_data_file_name)