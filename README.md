# PBI-insights
To unlock the business logic and narratives hidden within Power BI reports, 
this project aims to transform opaque .pbix files into a transparent, 
understandable, and queryable source of information, empowering both technical
and non-technical users to fully comprehend the data and calculations driving 
their business intelligence.

## Overview
A PowerBI file can be unzipped into its 'hidden' folder structure. This folder 
contains multiple interesting files. 
- The 'Layout' file
- The 'DataModel' file

The Layout can be understood as the frontend that the user will see inside 
the report. It includes information about subpages, visuals and created Measures.

There is also a DataModel file, which saves a lot more information like tables,
data fields.. the DataModel. We can picture it as the backend to make it easier.

## Idea
The plan is to start with a focus on the reports' pages, visuals and measures.
For all reports, the information is fetched from their Layout files and parsed
into excel (or csv) files. With that information, a user could see which measures
a report has, which ones are (un-) used etc.

With AI and the gathered "report-page-measure/fields" dependencies, we can create
descriptions of measures. With those, we can create descriptions of pages.

All the page descriptions will be embedded and saved in a vector DB.
By that, we create a search system for new users that want to find 
certain information in the totality of all reports and their subpages. 

## Roadmap
- [x] Pbix Unzipper
- [x] Layout Parser
- [x] Export of fetched data into excel files
- [x] AI descriptions/summaries for fetched data
- [x] Vector DB 
- [ ] DataModel Parser