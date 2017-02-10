
# -> ./doc/data_dictionary_trip_records_yellow.pdf

library(RevoScaleR)

setwd('C:/Users/Daniel/Svn/GitHub/r-server-introduction.git/trunk/data')

mht_xdf <- RxXdfData('mht_lab2.xdf')

rxGetInfo(mht_xdf, getVarInfo = TRUE, numRows = 10)

# Q1
formula_1 <- as.formula(tip_percent ~ trip_duration + pickup_dow:pickup_hour)
linmod_1 <- rxLinMod(formula_1, data = mht_xdf, dropFirst = TRUE, covCoef = TRUE)
summary(linmod_1)

# Q2
formula_2 <- as.formula(tip_percent ~ payment_type_desc + trip_duration + pickup_dow:pickup_hour)
linmod_2 <- rxLinMod(formula_2, data = mht_xdf, dropFirst = TRUE, covCoef = TRUE)
summary(linmod_2)

# Q3
prediction_1 <- rxPredict(linmod_1, mht_xdf, predVarNames = "tip_pred_1")
prediction_2 <- rxPredict(linmod_2, mht_xdf, predVarNames = "tip_pred_2")

rxHistogram(~tip_percent, mht_xdf, startval = 0, endVal = 50)
rxHistogram(~tip_pred_1, mht_xdf)
rxHistogram(~tip_pred_2, mht_xdf)
