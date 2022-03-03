# Code for genderization & data curation (before Collaboration, Productivity & NPI)

## MAG (Spark setup on HPC)

The MAG is supplied my Microsoft Academic as a directory of text-files, some of which are large (+50 gb). We use Spark SQL to interface with the MAG dataset for certain queries in the data curation process. The implementation allows for easy adaptation to various Spark environments, for instance on cloud services. As such, the technical infrastructure can be easily scaled up and down according to available resources. 

The use of Spark SQL over native PySpark methods is aimed at providing an interface with the MAG dataset which is convenient for people familiar with relational databases. With a little adaptation, the queries used should be able to run on other relational databases, such as MySQL or PostgreSQL. 


## Files

**MicrosoftAcademicGraph**   
Located in `MAG.py`, this class is a modification of code provided by Microsoft to access the MAG in an Azure environment. 
Its main responsibility is loading datasets stored in text-files as Spark DataFrames to be accessed with Pyspark and Spark SQL.  
This class takes a Pyspark SparkSession instance in its constructor. 

**MAGspark.py**  
This file defines the Spark configuration used to access files with Pyspark. It allows for a single-node client as well as a cluster setup with multiple nodes.



### Main Files 

**PreprocessingAllDocType**: Preprocessing of MAG data. E.g. creating convenience files and filtering by year (after 2010).

**PreprocessingFilters**: Further filtering of data (e.g. by DocType and whether papers are cited and/or reference other papers).

---
### Other Files (sanity checks)

**PreprocessingReproduce**: Checks whether we can reproduce results from Lasse (sanity check).
 
**PapersDocType**: Checks distribution of scientific papers on different DocTypes and the volume of papers by year.

**NumberOfPapers**: Checks volume of papers by year, and by different filters that we have applied to the data.
 
**FiltersValidate**: Checks that our filters are reasonable in terms of volume of data.  
