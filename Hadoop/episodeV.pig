inputDialouges = LOAD 'hdfs://localhost:9000/user/root/inputs/EpisodeV_dialouges.txt' USING PigStore('\t') AS (name:chararray, line:chararray);
ranked = RANK inputDialouges;
OnlyDialouges = FILTER ranked BY (ranked_inputDialouges > 2);
groupByName = GROUP OnlyDialouges BY name;
names = FOREACH groupByName GENERATE $0 as name, COUNT($1) as no_of_lines;
namesOrdered = ORDER names BY no_of_lines DESC;
Store namesOrdered INTO 'hdfs://localhost:9000/user/root/outputs/episodeV_output' USING PigStorage ('\t')