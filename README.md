# Data Warehouse Client

This package provides access to the e-Science Central data warehouse that can be used to store, access  and analyse 
data collected in scientific studies, including for healthcare applications. The primary aim of the warehouse
was to create a general system that enables users to explore data collected in a variety of forms. This might include
data collected through questionnaires, data collected from sensors, 
and features extracted from the analysis of sensor data (e.g. activity levels derived from raw accelerometer data). 
Researchers might wish to slice, dice, visualise, analyse and explore this data in different ways, 
e.g. all results for one participant,
all results for one type of measure in a study, 
changes in measurements over time. Others may wish to build models that can then be used in applications
that make predictions about future values.

Traditionally, data collected in studies has been stored in a collection of files, 
often with metadata encoded in the filenames. 
This makes it difficult, and time consuming, for researchers to explore, interpret and analyse the data.
The data warehouse exploits modern database technology to vastly simplify this effort. 
In doing this we have drawn heavily on the best practice for data warehouse design. 
However, there is more variety in the types of healthcare data to be stored than there is in a typical warehouse,
and so we have been forced to deviate from a conventional data warehouse in some aspect of the design.  
There are three guiding principles behind the design:
1.	The data warehouse must be able to store any type of data collected in a study without modifying the schema. 
This means that when new types of data are collected in studies (e.g. from a new questionnaire, 
a new data analysis program, or a new sensor) they can be stored in the warehouse without any changes to its design. 
This has 3 main advantages: 
firstly, it enables us to fix and optimise the schema for the tables in which the data is stored; 
secondly it means that applications and tools (e.g. for analysis and visualisation) 
built on the warehouse do not have to be updated when new types of data are added; 
thirdly, a single, multi-tenant database server can support many studies. 
This reduces the overall costs, the start-up time for a new study, and the overheads of managing the warehouse.
2.	Descriptive information about the types of measurement is stored in the warehouse so that tools or humans 
can interpret the data stored there.
3.	The design is optimised for query performance. In several cases, this has led to denormalization
 (duplication of data) to reduce the need for expensive joins.
4.	It must support a security regime to restrict each user’s access 
to the data collected in studies.

# Running Instructions

To install from PyPi, run:

pip install data-warehouse-client

In directory in which your executable is run, create a `db-credentials.json` file containing database 
credentials (substituting all `<VARS>`):
   ```
   {"user": "<USER>", "pass": "<PASSWORD>", "IP": "<IP>", "port": <PORT>}
   ```

