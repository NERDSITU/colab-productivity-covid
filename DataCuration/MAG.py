# -*- coding: utf-8 -*-
"""
Created on Mon Feb 22 06:05:03 2021

@author: lasse

## extended and edited by:
@author: victor
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import *

class MicrosoftAcademicGraph(object):
  # constructor
  def __init__(self, spark, data_folderpath="/home/vicp/data/mag2021_12_06"):
    # AzureStorageAccess.__init__(self, container, account, sas, key) 
    self.data_folderpath = data_folderpath
    self.spark = spark

  datatypedict = {
    'bool' : BooleanType(),
    'int' : IntegerType(),
    'uint' : IntegerType(),
    'long' : LongType(),
    'ulong' : LongType(),
    'float' : FloatType(),
    'string' : StringType(),
    'DateTime' : DateType(),
  }

  # return stream schema
  def getSchema(self, streamName):
    schema = StructType()
    for field in self.streams[streamName][1]:
      fieldname, fieldtype = field.split(':')
      nullable = fieldtype.endswith('?')
      if nullable:
        fieldtype = fieldtype[:-1]
      schema.add(StructField(fieldname, self.datatypedict[fieldtype], nullable))
    return schema

  # return stream dataframe
  def getDataframe(self, streamName):

    df = self.spark.read.format('csv').options(header='false', delimiter='\t').schema(self.getSchema(streamName))\
           .load(self.data_folderpath + self.streams[streamName][0])
    # create temporary view for streamName
    df.createOrReplaceTempView(streamName)
    return df

  def getSubset(self, streamName, columns):
    df = self.getDataframe(streamName)
    # select subset of columns
    df = df.select(columns)
    return df 

  def saveFile(self, df, folderName, fileName): 
    destination = f"/home/vicp/data/mag2021_12_06/{folderName}/{fileName}"
    df.write.option("sep", "\t").option("encoding", "UTF-8")\
    .csv(destination)

  def query_sql(self, query):
    """ 
    Executes Spark SQL query and returns DataFrame with results.
    Assumes views are created prior to execution
    """
    df = self.spark.sql(query)
    return df

  # define stream dictionary
  streams = {
# main files    
'Affiliations' : ('mag/Affiliations.txt', ['AffiliationId:long', 'Rank:uint', 'NormalizedName:string', 'DisplayName:string', 'GridId:string', 'OfficialPage:string', 'WikiPage:string', 'PaperCount:long', 'PaperFamilyCount:long', 'CitationCount:long', 'Iso3166Code:string', 'Latitude:float?', 'Longitude:float?', 'CreatedDate:DateTime']),
    'AuthorExtendedAttributes' : ('mag/AuthorExtendedAttributes.txt', ['AuthorId:long', 'AttributeType:int', 'AttributeValue:string']),
    'Authors' : ('mag/Authors.txt', ['AuthorId:long', 'Rank:uint', 'NormalizedName:string', 'DisplayName:string', 'LastKnownAffiliationId:long?', 'PaperCount:long', 'PaperFamilyCount:long', 'CitationCount:long', 'CreatedDate:DateTime']),
    'ConferenceInstances' : ('mag/ConferenceInstances.txt', ['ConferenceInstanceId:long', 'NormalizedName:string', 'DisplayName:string', 'ConferenceSeriesId:long', 'Location:string', 'OfficialUrl:string', 'StartDate:DateTime?', 'EndDate:DateTime?', 'AbstractRegistrationDate:DateTime?', 'SubmissionDeadlineDate:DateTime?', 'NotificationDueDate:DateTime?', 'FinalVersionDueDate:DateTime?', 'PaperCount:long', 'PaperFamilyCount:long', 'CitationCount:long', 'Latitude:float?', 'Longitude:float?', 'CreatedDate:DateTime']),
    'ConferenceSeries' : ('mag/ConferenceSeries.txt', ['ConferenceSeriesId:long', 'Rank:uint', 'NormalizedName:string', 'DisplayName:string', 'PaperCount:long', 'PaperFamilyCount:long', 'CitationCount:long', 'CreatedDate:DateTime']),
    'EntityRelatedEntities' : ('advanced/EntityRelatedEntities.txt', ['EntityId:long', 'EntityType:string', 'RelatedEntityId:long', 'RelatedEntityType:string', 'RelatedType:int', 'Score:float']),
    'FieldOfStudyChildren' : ('advanced/FieldOfStudyChildren.txt', ['FieldOfStudyId:long', 'ChildFieldOfStudyId:long']),
    'FieldOfStudyExtendedAttributes' : ('advanced/FieldOfStudyExtendedAttributes.txt', ['FieldOfStudyId:long', 'AttributeType:int', 'AttributeValue:string']),
    'FieldsOfStudy' : ('advanced/FieldsOfStudy.txt', ['FieldOfStudyId:long', 'Rank:uint', 'NormalizedName:string', 'DisplayName:string', 'MainType:string', 'Level:int', 'PaperCount:long', 'PaperFamilyCount:long', 'CitationCount:long', 'CreatedDate:DateTime']),
    'Journals' : ('mag/Journals.txt', ['JournalId:long', 'Rank:uint', 'NormalizedName:string', 'DisplayName:string', 'Issn:string', 'Publisher:string', 'Webpage:string', 'PaperCount:long', 'PaperFamilyCount:long', 'CitationCount:long', 'CreatedDate:DateTime']),
    'PaperAbstractsInvertedIndex' : ('nlp/PaperAbstractsInvertedIndex.txt.{*}', ['PaperId:long', 'IndexedAbstract:string']),
    'PaperAuthorAffiliations' : ('mag/PaperAuthorAffiliations.txt', ['PaperId:long', 'AuthorId:long', 'AffiliationId:long?', 'AuthorSequenceNumber:uint', 'OriginalAuthor:string', 'OriginalAffiliation:string']),
    'PaperCitationContexts' : ('nlp/PaperCitationContexts.txt', ['PaperId:long', 'PaperReferenceId:long', 'CitationContext:string']),
    'PaperExtendedAttributes' : ('mag/PaperExtendedAttributes.txt', ['PaperId:long', 'AttributeType:int', 'AttributeValue:string']),
    'PaperFieldsOfStudy' : ('advanced/PaperFieldsOfStudy.txt', ['PaperId:long', 'FieldOfStudyId:long', 'Score:float']),
    'PaperMeSH' : ('advanced/PaperMeSH.txt', ['PaperId:long', 'DescriptorUI:string', 'DescriptorName:string', 'QualifierUI:string', 'QualifierName:string', 'IsMajorTopic:bool']),
    'PaperRecommendations' : ('advanced/PaperRecommendations.txt', ['PaperId:long', 'RecommendedPaperId:long', 'Score:float']),
    'PaperReferences' : ('mag/PaperReferences.txt', ['PaperId:long', 'PaperReferenceId:long']),
    'PaperResources' : ('mag/PaperResources.txt', ['PaperId:long', 'ResourceType:int', 'ResourceUrl:string', 'SourceUrl:string', 'RelationshipType:int']),
    'PaperUrls' : ('mag/PaperUrls.txt', ['PaperId:long', 'SourceType:int?', 'SourceUrl:string', 'LanguageCode:string']),
    'Papers' : ('mag/Papers.txt', ['PaperId:long', 'Rank:uint', 'Doi:string', 'DocType:string', 'PaperTitle:string', 'OriginalTitle:string', 'BookTitle:string', 'Year:int?', 'Date:DateTime?', 'OnlineDate:DateTime?', 'Publisher:string', 'JournalId:long?', 'ConferenceSeriesId:long?', 'ConferenceInstanceId:long?', 'Volume:string', 'Issue:string', 'FirstPage:string', 'LastPage:string', 'ReferenceCount:long', 'CitationCount:long', 'EstimatedCitation:long', 'OriginalVenue:string', 'FamilyId:long?', 'FamilyRank:uint?', 'CreatedDate:DateTime']),
    'RelatedFieldOfStudy' : ('advanced/RelatedFieldOfStudy.txt', ['FieldOfStudyId1:long', 'Type1:string', 'FieldOfStudyId2:long', 'Type2:string', 'Rank:float']),
    
### all doctypes (early preprocessing) ###

'ProjectAuthorsAllDocType': ('datacuration/ProjectAuthorsAllDocType.txt', ['AuthorId:long', 'DisplayName:string', 'Country:string?', 'Gender:string?', 'Genderized:int?', 'MinDate:DateTime?', 'MinDateStem:DateTime?']),

'PaperAuthorAffiliationsAttributesAllDocType': ('datacuration/PaperAuthorAffiliationsAttributesAllDocType.txt', ['PaperId:long', 'AuthorId:long', 'Date:DateTime?', 'Year:int?', 'Month:int?', 'Quarter:int?', 'Gender:int', 'ScientificAge:float?', 'CountryCode:string?']),

'AuthorScientificAgeAllDocType': ('datacuration/AuthorScientificAgeAllDocType.txt', ['AuthorId:long', 'MinDateStem:DateTime?', 'MinDate:DateTime?']),

'ProjectPapersAllDocType': ('datacuration/ProjectPapersAllDocType.txt', ['PaperId:long', 'FamilyId:long?', 'FieldOfStudyId:long', 'DocType:string?', 'Date:DateTime?', 'PubOrderInFamily:int', 'NumAuthors:int']),

### (needed subsets) ###

'PapersClean': ('datacuration/PapersClean.txt', ['PaperId:long']),
'Papers25': ('datacuration/Papers25.txt', ['PaperId:long']),
'PapersRefCite': ('datacuration/PapersRefCite.txt', ['PaperId:long']),

'AuthorsClean': ('datacuration/AuthorsClean.txt', ['AuthorId:long']),
'FoS': ('datacuration/FoS.txt', ['FieldOfStudyId:long', 'NormalizedName:string']), 

'ProjectPapersAll': ('datacuration/ProjectPapersAll.txt', ['PaperId:long', 'FieldOfStudyId:long','NormalizedName:string', 'DocType:string?', 'Date:DateTime?']),
'ProjectPapers25': ('datacuration/ProjectPapers25.txt', ['PaperId:long', 'FieldOfStudyId:long','NormalizedName:string', 'DocType:string?', 'Date:DateTime?']),
'ProjectPapersRefCite': ('datacuration/ProjectPapersRefCite.txt', ['PaperId:long', 'FieldOfStudyId:long','NormalizedName:string', 'DocType:string?', 'Date:DateTime?']),

'ProjectAuthorsAll': ('datacuration/ProjectAuthorsAll.txt', ['AuthorId:long', 'DisplayName:string', 'Country:string?', 'Gender:string?', 'Genderized:int?', 'MinDate:DateTime?']),

'PaperAuthorAffiliationsAttributesAll': ('datacuration/PaperAuthorAffiliationsAttributesAll.txt', ['PaperId:long', 'AuthorId:long', 'Date:DateTime?', 'Gender:int', 'ScientificAge:float?', 'CountryCode:string?', 'DocType:string?', 'FieldOfStudyId:long', 'NormalizedName:string']),
'PaperAuthorAffiliationsAttributes25': ('datacuration/PaperAuthorAffiliationsAttributes25.txt', ['PaperId:long', 'AuthorId:long', 'Date:DateTime?', 'Gender:int', 'ScientificAge:float?', 'CountryCode:string?', 'DocType:string?', 'FieldOfStudyId:long', 'NormalizedName:string']),
'PaperAuthorAffiliationsAttributesRefCite': ('datacuration/PaperAuthorAffiliationsAttributesRefCite.txt', ['PaperId:long', 'AuthorId:long', 'Date:DateTime?', 'Gender:int', 'ScientificAge:float?', 'CountryCode:string?', 'DocType:string?', 'FieldOfStudyId:long', 'NormalizedName:string']),
'PaperAuthorAffiliationsAttributesNoFilter': ('datacuration/PaperAuthorAffiliationsAttributesNoFilter.txt', ['PaperId:long', 'AuthorId:long', 'Date:DateTime?', 'Gender:int', 'ScientificAge:float?', 'CountryCode:string?', 'DocType:string?', 'FieldOfStudyId:long', 'NormalizedName:string']),

### repository ###

'ProjectPapersRepo': ('datacuration/ProjectPapersRepo.txt', ['PaperId:long', 'FieldOfStudyId:long', 'NormalizedName:string', 'Doctype:string?', 'Date:DateTime?']),

'PaperAuthorAffiliationsAttributesRepo': ('datacuration/PaperAuthorAffiliationsAttributesRepo.txt', ['PaperId:long', 'AuthorId:long', 'Date:DateTime?', 'Gender:int', 'ScientificAge:float?', 'CountryCode:string?', 'DocType:string?', 'FieldOfStudyId:long', 'NormalizedName:string']),

'PaperAuthorAffiliationsAttributesNoFilter': ('datacuration/PaperAuthorAffiliationsAttributesNoFilter.txt', ['PaperId:long', 'AuthorId:long', 'Date:DateTime?', 'Gender:int', 'ScientificAge:float?', 'CountryCode:string?', 'DocType:string?', 'FieldOfStudyId:long', 'NormalizedName:string']),


### (sanity checks) ###

'ProjectPapersRefCite': ('datacuration/ProjectPapersRefCite.txt', ['PaperId:long', 'FieldOfStudyId:long', 'NormalizedName:string', 'DocType:string?', 'Date:DateTime?']), 

'ProjectPapers25': ('datacuration/ProjectPapers25.txt', ['PaperId:long', 'FieldOfStudyId:long', 'NormalizedName:string', 'DocType:string?', 'Date:DateTime?']), 

}
