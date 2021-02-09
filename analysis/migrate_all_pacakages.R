#taken from: https://www.r-bloggers.com/2017/07/quick-way-of-installing-all-your-old-r-libraries-on-a-new-device/
#run in current laptop, pc
installed <- as.data.frame(installed.packages())
write.csv(installed, 'installed_previously.csv')

#run in new laptop, pc
installedPreviously <- read.csv('installed_previously.csv')
baseR <- as.data.frame(installed.packages())
toInstall <- setdiff(installedPreviously, baseR)
install.packages(toInstall)