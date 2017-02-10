setwd('C:/Users/Daniel/Svn/GitHub/r-server-introduction.git/trunk/data')

# GROUP2Q10
out <- 'airquality.xdf'
rxImport(airquality, out, overwrite = TRUE)

aq <- rxReadXdf(out)
rxCube(Temp ~ factor(Day, levels = 1:31):factor(Month, levels = 1:12), aq)

summary(aq)

# -> TODO
rxCrossTabs(Temp ~ factor(Day, levels = 1:31):factor(Month, levels = 1:12), aq, means = TRUE)
rxCrossTabs(Temp ~ factor(Day, levels = 1:31):factor(Month, levels = 1:12), aq)
rxCrossTabs(Temp ~ factor(Month, levels = 1:31):factor(Day, levels = 1:12), aq)
rxCrossTabs(Temp ~ factor(Day, levels = 1:31):factor(Month, levels = 1:12), aq, means = FALSE)

# GROUP2Q07
hist(airquality$Temp, freq = TRUE, breaks = 10)
rxHistogram( ~ Temp, aq, breaks = 10)
rxHistogram( ~ Temp, aq, histType = "Percent", numBreaks = 10)
rxHistogram( ~ Temp, aq, histType = "Percent", breaks = 10)
rxHistogram( ~ Temp, aq, numBreaks = 10)

# GROUP2Q11 -> TODO
newLevels <- c("1973","1974","1975")
rxFactors("airquality.xdf", outFile = "airquality.xdf", 
          factorInfo = list(Year = list(newLevels = unique(newLevels))))

# GROUP2Q16
z <- airquality[,c("Wind","Temp")] 
cl <- kmeans(z, 2) 
cl$cluster

cl1 <- rxKmeans(~Wind+Temp,data=z,centers=2)
cl2 <- rxKmeans(~Wind+Temp,data=z,reportProgress=2)
cl3 <- rxKmeans(~Wind+Temp,data=z,numClusters=2)
cl4 <- rxKmeans(~Wind+Temp,data=z,maxIterations=2)
summary(cl3)

# GROUP2Q14
library("UsingR")

regfit <- lm(sheight ~ fheight, data=father.son)

out <- "father.son.xdf"
rxImport(father.son, out, overwrite = TRUE)
fs <- rxReadXdf(out)

rxLinMod(sheight ~ fheight, data=fs)
rxLinMod(sheight ~ fheight, data=fs)
rxLinMod(sheight ~ fheight, data=RxXdfData(fs))
rxLinMod(sheight ~ fheight, data=father.son)

# GROUP2Q08
rxct <- rxCrossTabs(Temp ~ factor(Day, levels = 1:31):factor(Month, levels = 1:12), aq, means = TRUE) 
print(rxct)

rxct <- rxCrossTabs(Temp ~ factor(Day, levels = 1:31):factor(Month, levels = 1:12), aq) 
print(rxct)

rxct <- rxCrossTabs(Temp ~ factor(Month, levels = 1:12):factor(Day, levels = 1:31), "airquality.xdf", means = TRUE) 
print(rxct)

rxct <- rxCrossTabs(Temp ~ factor(Day, levels = 1:31):factor(Month, levels = 1:12), aq) 
print(rxct, output = "means")
