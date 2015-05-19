/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2013-2014 Continuent Inc.
 * Contact: tungsten@continuent.org
 *
 * Load script for HDFS using cassandra utility.  Tables load to corresponding
 * directories in HDFS.  
 */

// Called once when applier goes online. 
function prepare()
{
    cassandra_base = '/tmp';
}

// Called at start of batch transaction. 
function begin()
{
  // Does nothing. 
}

// Appends data from a single table into a file within an HDFS directory. 
function apply(csvinfo)
{
  // Collect useful data. 
  sqlParams = csvinfo.getSqlParameters();
  csv_file = sqlParams.get("%%CSV_FILE%%");
    logger.info("Cassandra loader: CSV FILE: " + csv_file);
  schema = csvinfo.schema;
  table = csvinfo.table;
  seqno = csvinfo.startSeqno;
    logger.info(csvinfo);
  logger.info("Writing file: " + csv_file + " to cassandra");

  // Load file to Cassandra
    runtime.exec("/opt/continuent/share/applycqlsh.sh " + schema + ' "copy staging_' + table + " (optype,seqno,fragno,id,message) from '" + csv_file + "';\"");
    logger.info("/opt/continuent/share/applycqlsh.sh " + schema + ' "copy staging_' + table + " (optype,seqno,fragno,id,message) from '" + csv_file + "';\"");
}

// Called at commit time for a batch. 
function commit()
{
    runtime.exec("/opt/continuent/share/merge.rb " + schema);
}

// Called when the applier goes offline. 
function release()
{
  // Does nothing. 
}
