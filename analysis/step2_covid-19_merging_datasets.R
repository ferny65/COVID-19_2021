#-------Establecer carpeta de trabajo------------
workdir<-"/Volumes/Users/Usuarios/fernandolopez/Dropbox/COVID-19-PROYECTO/COVID-19-PROYECTO"
setwd(workdir)

#---------------Salvando ficheros modificados de municipios y de México-----------
municipios_file_name <- "datos_socioeconomicos_municipios.csv"
data_municipios <- read.csv(file=municipios_file_name, header = TRUE,stringsAsFactors = FALSE)
#actual_data_file_name <- createActualDataFileName(correction)
actual_data_file_name <- "201123COVID19MEXICO.csv"
data_mexico <- read.csv(file=actual_data_file_name,header = TRUE,stringsAsFactors = FALSE)

#-------------Combinando ambos conjuntos de datos------------------------------------
#-------------Generando una clave común para ambos ficheros---------------------------
data_mexico$clave <- with(data_mexico, paste(deENTIDAD_RES,deMUNICIPIOS))
data_municipios$clave <- with(data_municipios, paste(Estado,Municipio))

#Empleando la función merge para realizar un left inner join empleando los nombres o descropciones de municipios como campo clave
#Código tomado de: https://stackoverflow.com/questions/1299871/how-to-join-merge-data-frames-inner-outer-left-right

#full_data_mexico <- merge(x=data_mexico,y=data_municipios,by.x="deMUNICIPIOS",by.y="Municipio", all.x=TRUE, all.y=FALSE, sort=FALSE)
full_data_mexico <- merge(x=data_mexico,y=data_municipios,by="clave", all.x=TRUE, all.y=FALSE, sort=FALSE)

#-------------Guardando fichero con todos los datos ------------------------
actual_data_file_name <- "datos_socio_economicos_mexico.csv"
write.csv(full_data_mexico,file=actual_data_file_name)
