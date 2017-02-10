# DAT213x - Analyzing Big Data with Microsoft R Server

Introduction to Microsoft R Server using the RevoScaleR packages using some NYC taxi trip datasets.  
http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml

##Basic functionality of R server using the client package RevoScaleR
* rxImport, RrxXdfData, RxTextData -> reading files
* rxGetInfo -> display basic information about XDF files
* rxSummary -> univariate summaries of objects within XDF files
* rxDataStep -> data transformation
* rxSplit -> split dataset into multiple sets
* rxCube, rxCrossTab -> contingency tables
* rxHistogram -> histogram plots
* rxFactors -> factor variable recording
* rxKmeans -> k-means clustering
* rxLinMod -> linear models
* rxDTree -> parallel external memory algorithm for classification and regression trees
* rxDForest -> parallel external memory algorithm for classification and regression decision forests
* rxPredict -> compute predicted values and residuals
* rxQuantile -> approximate quantiles

##Topics covered
* file handling
* simple and complex transformation
* examining datasets
* examining outliers by filtering
* create sample datasets
* visualize data using tabs, plots and maps
* showing trends and distribution using plots
* reorder factor levels
* creating clusters
* split files in order to create training data
* create and examine linear model predictings
* compare predictions
* judging performance aspects on predictions
