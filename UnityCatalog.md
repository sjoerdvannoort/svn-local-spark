## Build 
After downloading the UnityCatalog repository from https://github.com/unitycatalog/unitycatalog build by running from the root of the local repository:
```
sbt pacakge
```
Ignore the error building the python wheel. Server, CLI, example tables and UI should be build.

## Running the Unity Catalog Server
To run the server and include the class path, run from the root (or create a cmd file):
```
 java -cp @server\target\classpath io.unitycatalog.server.UnityCatalogServer
```

## Running the CLI
```
java -cp @examples\cli\target\classpath io.unitycatalog.cli.UnityCatalogCli %~1 %~2 %~3 %~4 %~5 %~6
```
## Running the UI
from the UI directory, run
```
yarn start
```