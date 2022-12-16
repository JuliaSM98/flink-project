# flink-project
* Pablo Crucera Barrero
* Júlia Sánchez Martínez

In order to run the application, the following procedure must be followed from the root folder of the project: 
* mvn clean package -Pbuild-jar
* $PATH_TO_/flink-1.14.0/bin/flink run -p 3 -c master.VehicleTelematics.Main target/flinkProgram-1.0-SNAPSHOT.jar $PATH_TO_INPUT_FILE $PATH_TO_OUTPUT_FOLDER



