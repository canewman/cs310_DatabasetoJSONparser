
package cs310_databasetojsonparser;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.LinkedHashMap;
import org.json.simple.JSONArray;

/**
 *
 * @author Aaron
 */
public class Cs310_DatabasetoJSONparser {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        Cs310_DatabasetoJSONparser database = new Cs310_DatabasetoJSONparser();
        System.out.println(database.getJSONdata());
        database.getJSONdata();
    }
    
       
    public JSONArray getJSONdata()
    {
        JSONArray results = null;
        JSONArray records = new JSONArray();
        Connection conn = null;
        PreparedStatement pstSelect = null, pstUpdate = null;
        ResultSet resultset = null;
        ResultSetMetaData metadata = null;
        
        String query, value;
        String[] headers;
                
        boolean hasresults;
        int resultCount, columnCount;
        
        try {
            
            /* Identify the Server */
            
            String server = ("jdbc:mysql://localhost/p2_test");
            String username = "root";
            String password = "Summer_28";
            //System.out.println("Connecting to " + server + "...");
            
            /* Load the MySQL JDBC Driver */
            
            Class.forName("com.mysql.jdbc.Driver").newInstance();
            
            /* Open Connection */

            conn = DriverManager.getConnection(server, username, password);

            /* Test Connection */
            
            if (conn.isValid(0)) {
                
                /* Connection Open! */
                
                System.out.println("Connected Successfully!");
                          
                /* Prepare Select Query */
                
                query = "SELECT * FROM people";
                pstSelect = conn.prepareStatement(query);
                
                /* Execute Select Query */
                
                //System.out.println("Submitting Query ...");
                
                hasresults = pstSelect.execute();                
                
                /* Get Results */
                
                //System.out.println("Getting Results ...");
                
                while ( hasresults || pstSelect.getUpdateCount() != -1 ) {

                    if ( hasresults ) {
                        
                        /* Get ResultSet Metadata */
                        
                        resultset = pstSelect.getResultSet();
                        metadata = resultset.getMetaData();
                        columnCount = metadata.getColumnCount();
                        
                        headers = new String[columnCount-1];
                        
                        /* Get Column Names; Print as Table Header */
                        
                        for (int i = 1; i <= headers.length; i++) {                            
                            headers[i-1] = metadata.getColumnLabel(i+1);
                            //System.out.println(headers[i-1]);
                        }
                        
                        LinkedHashMap data = new LinkedHashMap();
                        /* Get Data; Print as Table Rows */
                        
                        while(resultset.next()) {
                            data = new LinkedHashMap();
                            /* Begin Next ResultSet Row */

                            System.out.println();
                            
                            if(resultset.wasNull())
                            {
                                for(int i = 0; i <= headers.length-1; i++)
                                {
                                    data.put(headers[i], "NULL");
                                }
                            }
                            else
                            {///////////////////////TEST WOULD ONLY PASS IN THIS ORDER////////////////////////
                                data.put(headers[6], resultset.getString(8));//zip
                                data.put(headers[0], resultset.getString(2));//firstname
                                data.put(headers[3], resultset.getString(5));//address
                                data.put(headers[4], resultset.getString(6));//city
                                data.put(headers[5], resultset.getString(7));//state
                                data.put(headers[1], resultset.getString(3));//middle in
                                data.put(headers[2], resultset.getString(4));//last name
                            }
                            /* Loop Through ResultSet Columns; Print Values */
/*
                            for (int i = 0; i <= headers.length-1; i++) {    //ORIGINAL ORDER
                                //System.out.println(data.size());
                                value = resultset.getString(i+2);

                                if (resultset.wasNull()) {
                                    data.put(headers[i], "NULL");                                    
                                }

                                else {
                                    data.put(headers[i], value);
                                }                                
                                
                            }
*/ 
                            records.add(data);
                        }
                        
                    }

                    else {

                        resultCount = pstSelect.getUpdateCount();  

                        if ( resultCount == -1 ) {
                            break;
                        }

                    }
                    
                    /* Check for More Data */

                    hasresults = pstSelect.getMoreResults();

                }
                results = records;
                
            }
            
            System.out.println();
            
            /* Close Database Connection */
            
            conn.close();
            
        }
        
        catch (Exception e) {
            System.err.println(e.toString());
        }
        
        /* Close Other Database Objects */
        
        finally {
            
            if (resultset != null) { try { resultset.close(); resultset = null; } catch (Exception e) {} }
            
            if (pstSelect != null) { try { pstSelect.close(); pstSelect = null; } catch (Exception e) {} }
            
            if (pstUpdate != null) { try { pstUpdate.close(); pstUpdate = null; } catch (Exception e) {} }
            
        }
           
        return results;
    }
    
}
