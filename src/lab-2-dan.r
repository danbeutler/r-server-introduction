
# -> ./doc/data_dictionary_trip_records_yellow.pdf

library(RevoScaleR)

setwd('C:/Users/Daniel/Svn/GitHub/r-server-introduction/trunk/data')

mht_xdf <- RxXdfData('mht_lab2.xdf')

rxGetInfo(mht_xdf, getVarInfo = TRUE, numRows = 10)

# Q1
rxCube( ~ Ratecode_type_desc:payment_type_desc, mht_xdf)

# Q2
rxCrossTabs( ~ distance:duration:payment_type_desc, mht_xdf,
             transforms = list(
               distance = ifelse(trip_distance <= 5, 0, 1),
               duration = ifelse(trip_duration / 60 <= 10, 0, 1),
               distance = factor(distance, labels = c('short', 'long')),
               duration = factor(duration, labels = c('short', 'long'))
           )
)

# Q3
rxHistogram( ~ tip_percent, mht_xdf, histType = "Counts", numBreaks = 20)

# Q4
rxHistogram( ~ tip_percent, mht_xdf, histType = "Counts", numBreaks = 20, rowSelection = (payment_type_desc == 'card'))

# Q5
rxHistogram( ~ tip_percent, mht_xdf, histType = "Counts", numBreaks = 20, rowSelection = (payment_type_desc == 'cash'))

# Q6
rxHistogram( ~ group_percent, mht_xdf, histType = "Counts",
             rowSelection = (payment_type_desc == 'card'),
             transforms = list(
               group_percent = cut(tip_percent,
                                   breaks = c(-10, 5, 10, 15, 20, 25, 100))
             )
)

# Q7
rxHistogram( ~ group_percent | Ratecode_type_desc, mht_xdf, histType = "Percent",
             rowSelection = (payment_type_desc == 'card'),
             transforms = list(
               group_percent = cut(tip_percent,
                                   breaks = c(-10, 5, 10, 15, 20, 25, 100))
             )
)
