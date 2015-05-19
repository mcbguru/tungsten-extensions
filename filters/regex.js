var stcmatch = {};
var regexpattern;
var replacestring;

function prepare()
{
    logger.info("regexp: Initializing...");  

    stcspec = filterProperties.getString("stcspec");
    stcarray = stcspec.split(",");
    for(i=0;i<stcarray.length;i++) { 
	stcmatch[stcarray[i]] = 1;
    }

    var searchpattern = filterProperties.getString("searchpattern");
    regexpattern = new RegExp(searchpattern, "mg");
    replacepattern = filterProperties.getString("replacepattern");
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
	  execregex(event, d);
      }
    }
  }
}

function execregex(event, d)
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
	    
	    logger.info("regexp: Checking whether to apply " + rowchangestc);  
	    if (rowchangestc in stcmatch) {
		
		for (row = 0; row < columnValues.size(); row++)
		{
		    values = columnValues.get(row);
		    value = values.get(c);
		    valueString = new String(value.getValue());
		    logger.info("regexp: Translating string " + valueString.valueOf());  
		    newstring = valueString.replace(regexpattern,replacepattern);
		    value.setValue(newstring);
		    logger.info("regexp: Translated string " + newstring);  
		}
	    }
	}
    }
}
