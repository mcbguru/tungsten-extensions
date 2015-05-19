var stcmatch = {};

function prepare()
{
    logger.info("xmltojson: Initializing...");  

    stcspec = filterProperties.getString("stcspec");
    stcarray = stcspec.split(",");
    for(i=0;i<stcarray.length;i++) { 
	stcmatch[stcarray[i]] = 1;
    }
}

function filter(event)
{
  data = event.getData();
  if(data != null)
  {
    for (i = 0; i < data.size(); i++)
    {
      d = data.get(i);
  
      if (d != null && d instanceof com.continuent.tungsten.replicator.dbms.StatementData)
      {

      }
      else if (d != null && d instanceof com.continuent.tungsten.replicator.dbms.RowChangeData)
      {
	  tojson(event, d);
      }
    }
  }
}

function tojson(event, d)
{
    rowChanges = d.getRowChanges();
    
    for(j = 0; j < rowChanges.size(); j++)
    {
	oneRowChange = rowChanges.get(j);
	var schema = oneRowChange.getSchemaName();
	var table = oneRowChange.getTableName();
	var columns = oneRowChange.getColumnSpec();
	columnValues = oneRowChange.getColumnValues();
	for (c = 0; c < columns.size(); c++)
	{
	    columnSpec = columns.get(c);
	    columnname = columnSpec.getName();
	    rowchangestc = schema + '.' + table + '.' + columnname;
	    
	    if (rowchangestc in stcmatch) {
		
		for (row = 0; row < columnValues.size(); row++)
		{
		    values = columnValues.get(row);
		    value = values.get(c);
		    valueString = value.getValue();
		    xmlDoc = new XML(valueString);
		    xmlDoc.hasChildNodes();
		    value.setValue(JSON.stringify(xmlDoc));
		}
	    }
	}
    }
}

