# DAT213x - Analyzing Big Data with Microsoft R Server

Introduction to Microsoft R Server using the RevoScaleR packages using some NYC taxi trip datasets.  

For detailed information about this great EDX course by Microsoft go and visit:  
:point_right: https://courses.edx.org/courses/course-v1:Microsoft+DAT213x+3T2016/info

## Topics covered
* File handling
* Simple and complex transformation
* Examining datasets
* Examining outliers by filtering
* Create sample datasets
* Visualize data using tabs, plots and maps
* Showing trends and distribution using plots
* Reorder factor levels
* Creating clusters
* Split files in order to create training data
* Create and examine linear model predictings
* Compare predictions
* Judging performance aspects on predictions

## Basic functionality of R server using the client package RevoScaleR
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
