RetroChadSql : Version 0.9.0 : 2014 February 25

RetroChadSql is a Python program that will use Chadwick to parse Retrosheet play-by-play and will then put that data into a relational database.

You can tell RetroChadSql to do all or only some of the tasks it can do. Those tasks are:
--downloading the zipped event files from Retrosheet
--unzipping the zipped files
--having Chadwick assemble the data into CSV files
--writing SQL files that fit the newest versions of Chadwick and the event files, even if they're newer than RetroChadSQL.
--using the SQL files to load the data into a relational database

In RetroChadSql version 0.9.0, MySQL (along with its mimic, MariaDb) is the only database supported. Without MySQL, you can still do every step except loading the data into the database. You can also edit the SQL files to be compatible with other database engines.

Future versions of RetroChadSql will support PostgreSQL, SQLite, SQL Server, and generic ANSI-compliant SQL files that can be manually copied into databases that don't allow loading of files from the operating system command line, such as Access.

To run RetrochadSql, you will need to have Python 2.7 installed on your computer. If you have Windows, you may need to install Python. If you have Linux or Macintosh, you already have Python. If you have Python, you probably also have Tkinter and its related modules as part of Python. There are a few Linux builds, though, where you'll need to add Tkinter, ttk, tkFont, tkMessageBox, and ScrolledText yourself.

To assemble the data into CSV files and write the SQL files, you'll also need Chadiwck, which is at http://chadwick.sourceforge.net/doc/index.html . If you have Windows, the Chadwick tools are ready for you to download and for RetroChadSql to use--just click the "Pre-built command-line binaries for Microsoft Windows" link, unzip the folder that gets downloaded and put the Chadwick tools wherever you want on your computer. If you are running Macintosh or Linux, click the "Full source code" link and you'll have to compile Chadwick yourself.

For more information, contact info at alltimersports dot com
