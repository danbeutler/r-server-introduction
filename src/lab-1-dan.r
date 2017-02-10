
# -> ./doc/data_dictionary_trip_records_yellow.pdf

library(RevoScaleR)

setwd('C:/Users/Daniel/Svn/GitHub/r-server-introduction.git/trunk/data')

nyc_xdf <- RxXdfData('nyc_lab1.xdf')

rxGetInfo(nyc_xdf, getVarInfo = TRUE, numRows = 10)

rxSummary( ~ factor(RatecodeID, levels = 1:99) + 
             factor(payment_type, levels = 1:4), nyc_xdf)
