module elasticsearch.api.actions.bulk;

import elasticsearch.api.base;
import std.range, std.algorithm;

alias bulk = elasticsearch.api.actions.bulk.bulkIndex;


struct Records{
    Record[ulong] records;
    this(string[string][] data, string method, string index, string type){
      string[string] header = ["_index": index, "_type": type];
      foreach(i,rec; data){
        auto record = Record(method, header, rec );
        records[i] = record;
      }
    }

    string toString(){
      char[] result;
      records.each!(a=>result ~= a.toString());
      string r = result.dup;
      return r;
    }

}

struct Record {
  string method;
  string[string] header;
  string[string] data;

  string toString(){

    auto headerJson = Json.emptyObject;
    auto dataJson = Json.emptyObject;
    header.byPair().each!(a=>headerJson[a[0]] = a[1]);
    data.byPair().each!(a=>dataJson[a[0]] = a[1]);
    string result = ("{\"" ~ method   ~ "\" : " ~ headerJson.toString ~ "}\n" ~
      dataJson.toString ~"\n");
    return result; 
  } 
}



Response bulkIndex(Client client, Parameters arguments = Parameters()) {
    RequestMethod method;

    arguments.enforceParameter("index");
    arguments.enforceParameter("type");

    auto requestBody = arguments["body"];
    auto params = arguments.validateAndExtract(
        "consistency", "op_type", "parent", "percolate", "refresh", "replication", "routing",
        "timeout", "timestamp", "ttl", "version", "version_type","bulk"
    );
    string[] path = [arguments["index"], arguments["type"],"_bulk"];

    if (arguments.hasField("id")) {
        method = RequestMethod.PUT;
        path ~= arguments["id"];
    }
    else {
        method = RequestMethod.POST;
    }

    return client.performRequest(method, esPathify(path), params, requestBody);
}

Response bulkIndex(Client client, string indexName, string type, string[string][] dataAA, Parameters p = Parameters()) {
    auto data = Records(dataAA, "index", indexName, "cdr");
    p["index"] = indexName;
    p["type"] = type;
    p["body"] = data.toString();
    p["bulk"] = "true";
    return bulkIndex(client, p);
}

void bulkIndex(Client client, string indexName, string type, string[string][] dataAA,int doc_chunks, Parameters p = Parameters()) {
    import std.stdio;
    p["index"] = indexName;
    p["type"] = type;
    p["bulk"] = "true";
    
    //p["body"] = dataAA;
    foreach(chunk; std.range.chunks(dataAA, doc_chunks)){
        //writeln(chunk);
        //writeln("####################");
        auto data = Records(chunk, "index", indexName, "cdr");
        p["body"] = data.toString;
        bulkIndex(client, p);
    }
    //return bulkIndex(client, p);
}

Response bulkIndex(Client client, string indexName, string type, string requestBody,string method, Parameters p = Parameters()) {
    p["index"] = indexName;
    p["type"] = type;
    p["body"] = requestBody;
    p["bulk"] = "true";
    return bulkIndex(client, p);
}