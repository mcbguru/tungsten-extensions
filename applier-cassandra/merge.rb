#!/usr/bin/ruby

require 'cql'

client = Cql::Client.connect(hosts: ['192.168.1.51'])
client.use('sample')

rows = client.execute("SELECT id FROM staging_sample where optype = 'D'")

deleteids = Array.new()

rows.each do |row|
  puts "Found ID #{row['id']} has to be deleted"
  deleteids.push(row['id'])
end

deleteidlist = deleteids.join(",")

client.execute("delete from sample where id in (#{deleteidlist})");
puts("delete from sample where id in (#{deleteidlist})");
rows = client.execute("SELECT * FROM staging_sample where optype = 'I'");

updateids = Hash.new()
updatedata = Hash.new()

rows.each do |row|
  id = row['id']
  puts "Found ID #{id} seq #{row['seqno']} has to be inserted"
  if updateids[id]
    if updateids[id] < row['seqno']
      updateids[id] = row['seqno']
      row.delete('seqno')
      row.delete('fragno')
      row.delete('optype')
      updatedata[id] = row
    end
  else
    updateids[id] = row['seqno']
    row.delete('seqno')
    row.delete('fragno')
    row.delete('optype')
    updatedata[id] = row
  end
end

updatedata.each do |rowid,rowdata|
  puts "Should update #{rowdata['id']} with #{rowdata['message']}"
  collist = rowdata.keys.join(',')
  colcount = rowdata.keys.length
  substbase = Array.new()
#  (1..colcount).each {substbase.push('?')}
  rowdata.values.each do |value|
    if value.is_a?(String)
      substbase.push("'" + value.to_s + "'")
    else
      substbase.push(value)
    end
  end

  substlist = substbase.join(',')

  puts('Column list: ',collist)
  puts('Subst list: ',substlist)
  cqlinsert = "insert into sample ("+collist+") values ("+substlist+")"
  puts("Statement: " + cqlinsert)
  client.execute(cqlinsert)
end

client.execute("delete from staging_sample where optype in ('D','I')")
