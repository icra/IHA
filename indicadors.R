### Càlcul indicadors hidrològics

library(tidyverse)
library(lubridate)
library(IHA)
library(caTools)
library(plyr)

## Carregar series (o observacions o series simulades)

for (i in 1:73){
  if (!(i %in% c(2,6,10,14,15,27,32,56,63,64))){ # no carregar estacions que no fem servir
    if (i < 10){
      nom <- paste("a0", i, sep = "", collapse = "")
    } else {
      nom <- paste("a", i, sep = "", collapse = "")
    }
    read <- read.csv(paste("C:/Users/lverdura/ICRA/ACA - TRAÇA - General/Dades/Model SWAT+ CIC/7-Calibration hydrology/Gauging stations/Observations", paste(nom, "csv", sep = "."), sep = "/"))
    
    if (i == 1){
      obs <- tibble(date = as.Date(read$Date), read$Flow, .name_repair = "universal")
      noms <- c(names(obs)[1], nom)
    } else {
      obs <- add_column(obs, read$Flow, .name_repair = "universal")
      noms <- c(noms, nom)
    }
    
    if (i == 73){
      names(obs) <- noms
      obs <- filter(obs, date < "2021-01-01")
      remove(noms,i)
    }
    remove(read, nom)
  } 
}

a47 <- tibble(date = obs$date, q = obs$a47)  # única estació que no te cap NA
a47_zoo <- read.zoo(a47, format = "%Y-%m-%d")

a03 <- tibble(date = obs$date, q = obs$a03)  
a03_zoo <- read.zoo(a03, format = "%Y-%m-%d")


## IHA Group 2

g2_a47 <- group2(a47_zoo, "calendar")
g2_a03 <- group2(a03_zoo, "calendar") ### Problema amb NA: deixa de comptar si troba un NA. Per exemple, al 2004 la mitjana en 7 dies mínima hauria de ser 2.003 (19-25 oct), de després del NA del 7 d'octubre 
                                      ### 2008/2012: surt INF perquè el primer NULL és al 3/1 de gener, o sigui no arriba a calcular cap mitjana

## Això passa perque fins de group2 es fa servir la funció runmean (caTools) amb alg = 'fast', i per tant no pot treballar amb NA, però canviant-ho per alg = 'C' sí funciona igual que el càlcul ambn l'excel

group2_mod <- function(x, year = c('water', 'calendar'), mimic.tnc = T, ...){
  stopifnot(is.zoo(x), inherits(index(x), 'Date') | inherits(index(x), 'POSIXt'))
  year <- match.arg(year)
  yr <- switch(year,
               water = water.year(index(x)),
               calendar = year(index(x)))
  rollx <- runmean.iha_mod(x, year = yr, mimic.tnc = mimic.tnc)
  xd <- cbind(year = yr, as.data.frame(rollx))
  res <- ddply(xd, .(year), function(x) group2Funs(x[,-1]), ...)
  return(res)
}
runmean.iha_mod <- function(x, year = NULL, mimic.tnc = F){
  window <- c(1, 3, 7, 30, 90)
  vrunmean <- Vectorize(runmean, vectorize.args = 'k')
  if (mimic.tnc){
    sx <- split(coredata(x), year)
    rollx <- lapply(sx, vrunmean, k = window, alg = 'C', endrule = 'NA') # mod
    rollx <- do.call('rbind', rollx)
  } else {
    rollx <- vrunmean(coredata(x), k = window, alg = 'C', endrule = 'NA') # mod
  }
  colnames(rollx) <- sprintf('w%s', window)
  return(rollx)
}

g2_a03_mod <- group2_mod(a03_zoo, "calendar")


## Low and high flows (10th and 90th percentiles)

p_a47 <- a47 %>% 
  group_by(year(date)) %>% 
  summarize(p10 = quantile(q, .1, na.rm = T),
            p90 = quantile(q, .9, na.rm = T))

p_a03 <- a03 %>% 
  group_by(year(date)) %>% 
  summarize(p10 = quantile(q, .1, na.rm = T),
            p90 = quantile(q, .9, na.rm = T))

## IHA Group 3

g3_a47 <- group3(a47_zoo, "calendar")
g3_a03 <- group3(a03_zoo, "calendar")

## IHA Group 4

g4_a47 <- group4(a47_zoo, "calendar", thresholds = quantile(a47_zoo, c(.1, .9), na.rm = T))               ### Problema: fem servir els percentils 10 i 90 de tota la sèrie, no de cada any en concret. Sembla però que a l'IHA també ho deuen fer així?
g4_a03 <- group4(a03_zoo, "calendar", thresholds = quantile(a03_zoo, c(.1, .9), na.rm = T))

## IHA Group 5

g5_a47 <- group5(a47_zoo, "calendar")
g5_a03 <- group5(a03_zoo, "calendar")








