dfESP Component Project
=======================

This project is based on a template of a Camel component. 
This component provides integration of SAS Esp-Engine (ESP= Event Stream Processing also called (in Javadoc) "CEP" = Complex Event Processing)
The uri scheme (prefix) is dfESP - each Uri begins with dfESP://.


From original apache camel readme.

To build this project use

    mvn install

For more help see the Apache Camel documentation:

    http://camel.apache.org/writing-components.html
    
1.5 - added the mode dynamic which is needed for the lookup functionality (to allow upsert and deletes via the same endpoint).
1.6 - now using esp engine 2.3 (dfx-esp-api 2.3) and includes a fix for input streams containing backslashes (\) and double quotes (").