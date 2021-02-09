#1. Ejecutar antes de descargar el proyecto. 
#tomado de https://github.com/rstudio/renv
if (!requireNamespace("remotes"))
  install.packages("remotes")

remotes::install_github("rstudio/renv")

#2. Una vez descargado el proyecto y cargado en R Studio ejecutar en la consola de R: renv::init()
