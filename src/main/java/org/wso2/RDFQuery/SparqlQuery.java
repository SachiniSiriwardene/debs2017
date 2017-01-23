package org.wso2.RDFQuery;


import com.hp.hpl.jena.query.*;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import org.apache.jena.riot.RDFDataMgr;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;

/**
 * Created by ACER on 1/21/2017.
 */
public class SparqlQuery {

    private static ArrayList<String> str = new ArrayList<String>();
    private static Float[] dataArr = new Float[120];

    public static void main(String[] args) {
        String data ="";
        Model model = RDFDataMgr.loadModel("molding_machine_100M_rdf.ttl") ;
        String queryString = " PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> " +
                "PREFIX wmm: <http://www.agtinternational.com/ontologies/WeidmullerMetadata#>" +
                "PREFIX debs:<http://project-hobbit.eu/resources/debs2017#>" +
                "PREFIX ssn: <http://purl.oclc.org/NET/ssnx/ssn#>" +
                "PREFIX IoTCore: <http://www.agtinternational.com/ontologies/IoTCore#>" +
                "PREFIX i40: <http://www.agtinternational.com/ontologies/I4.0#>" +
                "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>"+
                "SELECT ?machine ?time ?timeStamp ?value ?property WHERE { " +
                    "?a rdf:type i40:MoldingMachineObservationGroup ." +
                    "?a i40:machine ?machine .  " +
                    "?a ssn:observationResultTime ?time ." +
                    "?time IoTCore:valueLiteral ?timeStamp ." +
                    "?a i40:contains ?b ." +
                    "?b ssn:observationResult ?c ." +
                    "?c ssn:hasValue ?d ." +
                    "?b ssn:observedProperty ?property ." +
                    "?d  IoTCore:valueLiteral ?value" +
                "}" +
                "ORDER BY ASC (?timeStamp)" ;
        Query query = QueryFactory.create(queryString) ;
        try {
            QueryExecution qexec = QueryExecutionFactory.create(query, model);
            ResultSet results = qexec.execSelect() ;
            results = ResultSetFactory.copyResults(results) ;

            //ResultSetFormatter.out(System.out, results, query) ;

            for ( ; results.hasNext() ; )
            {
                QuerySolution soln = results.nextSolution() ;
               // RDFNode x = soln.get("time") ;       // Get a result variable by name.
                Resource time = soln.getResource("time") ; // Get a result variable - must be a resource
                Resource property = soln.getResource("property");
                Resource machine = soln.getResource("machine");
                Literal value = soln.getLiteral("value") ;
                Literal timeStamp = soln.getLiteral("timeStamp");// Get a result variable - must be a literal

                if (value.toString().contains("#string")) {

                } else {
                    String newdata = machine.getLocalName()+","+time.getLocalName()+","+timeStamp.getValue();

                    //check if timestamp exists, if so append else create new
                    if(data.contains(newdata)){
                        //get the property number -> add value to respective place in the array
                        int num = Integer.parseInt(property.getLocalName().substring(1));
                        dataArr[num] = value.getFloat();
                       // System.out.println(num+"\t"+dataArr[num]);
                        //data = data + ","+property.getLocalName()+","+value.getFloat();

                    }
                    else {
                        for (int i = 0; i<120; i++){
                            if(dataArr[i]==null){
                                dataArr[i] =0.0F;
                            }
                            data = data+","+dataArr[i];
                        }
                        str.add(data);
                        data = machine.getLocalName()+","+time.getLocalName()+","+timeStamp.getValue();
                        //data = machine.getLocalName()+","+time.getLocalName()+","+timeStamp.getValue()+","+property.getLocalName()+","+value.getFloat();
                    }
                    //data = machine.getLocalName()+"\t"+time.getLocalName()+"\t"+timeStamp.getValue()+"\t"+property.getLocalName()+"\t\t"+value.getFloat()+"\n";


                }



               // System.out.println(r);
            }
        }catch(Exception e)
        {
            System.out.print(e);
        }


        for (int i = 0; i<120; i++){
            if(dataArr[i]==null){
                dataArr[i] =0.0F;
            }
            data = data+","+dataArr[i];
        }
        str.add(data);
        File file = new File("src/main/resources/rdfData_extract_100m.csv");
        FileWriter writer = null;
        // creates the file
       try {
            file.createNewFile();
            writer = new FileWriter(file);
                for (int i =1; i<str.size(); i++){
                   writer.write(str.get(i)+"\n");
                }

            writer.flush();
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
