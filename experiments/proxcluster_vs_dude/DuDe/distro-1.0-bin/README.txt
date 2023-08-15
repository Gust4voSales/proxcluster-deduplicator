Name: DuDe - The Duplicate Detection Framework
Version: 1.0
Date: 2011-09-13
Homepage: http://www.hpi.uni-potsdam.de/naumann/projekte/dude.html
====================================================================

You have downloaded the binary of the DuDe. This archive contains several files and libraries:

--- modules/core-1.0.jar ---

This library contains the core functionality of DuDe. 
Dependencies:
 - log4j-1.2.14.jar (used for logging)
 - jackson-core-lgpl-1.7.5.jar (used for generating and parsing Json)
 - simmetrics-1.6.2.jar (is needed, if any of the simmetrics SimilarityFunctions are used)
 - junit-4.8.2.jar (is needed, if any JUnit test shall be executed)
 - pojava-2.6.1.jar (is needed when using any of the predefined domain specific similarity functions)

--- modules/database-1.0.jar ---

This library contains the DataSource implementation for accessing databases.
Dependencies*:
 - mysql-connector-java-5.1.15.jar (if a MySQL database shall be accessed)
 - postgresql-9.0-801.jdbc4.jar (if a PostGreSQL database shall be accessed)

* DB2 and Oracle databases are also supported. If you want to use any of those database you have to add its 
  driver manually.
  
All mentioned dependencies can be found in the ./lib folder.