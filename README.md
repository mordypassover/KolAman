# KolAman

## basic flow

data enters the sistem thrwe CsFileSystemWatcher that colects data from simulator.
the data is desirealised to object, wich validates and filters bad data, then serialises
and publishes to kafka topic.
logs are sent to a difrent topic to be handeled and stored in elastic.

the python consumer colects the meseges caches ids to elimenate doplicets, validates and sends to rabit
by regen que.

the data is consumed by rabbit consumer and after basic validation is stored in sql.

## run sistem:

to  run sistem u must:
1. docker compose up
2. run -it --rm --name rabbitmq -p 5552:5552 -p 15672:15672 -p 5672:5672 -e         RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS="-rabbitmq_stream advertised_host localhost" rabbitmq:4-management
    

    docker exec rabbitmq rabbitmq-plugins enable rabbitmq_stream rabbitmq_stream_management
3. run bat file by  duble cliking

4. run CsFileSystemWatcher with dotnet run !
5. run py_consummer
6. run Rabit consumer

the paths  used in the program are not absloot bot thay do dipend on the spesific
file structer of the project!(and probbly works only for widows)

## validations
the validation of time and numeric tips hapin during desrilesation, uter validation is in python 


## db
i  chose mysql for its simplisety and becols the data is all prity much exactly the same 