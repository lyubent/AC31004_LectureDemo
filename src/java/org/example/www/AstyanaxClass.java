package org.example.www;

import com.google.common.collect.ImmutableMap;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.CqlResult;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;


public class AstyanaxClass {
    
    public Keyspace getContext() {
        AstyanaxContext<Keyspace> context = new AstyanaxContext.Builder()
            .forCluster("ClusterName")
            .forKeyspace("FirstKS")
            .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()      
                .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
                .setClock(null)
            )
            .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl("MyConnectionPool")
                .setPort(9160)
                .setMaxConnsPerHost(1)
                .setSeeds("127.0.0.1:9160")
            )
            .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
            .buildKeyspace(ThriftFamilyFactory.getInstance());

        context.start();
        Keyspace keyspace = context.getEntity();
        
        return keyspace;
    }
    
    public boolean createKS(){
        
        try {
            getContext().createKeyspace(ImmutableMap.<String, Object>builder()
            .put("strategy_options", ImmutableMap.<String, Object>builder()
                .put("replication_factor", "1")
                .build())
            .put("strategy_class",     "SimpleStrategy")
                .build()
             );
            
            return true;
        
        } catch (ConnectionException ex) {
            ex.printStackTrace();
            return false;
        }
    }
    
    public boolean createCF(){
        
        try {
            ColumnFamily<String, String> CF_STANDARD1 = ColumnFamily
                .newColumnFamily("Standard1", StringSerializer.get(),
                        StringSerializer.get());

            getContext().createColumnFamily(CF_STANDARD1, ImmutableMap.<String, Object>builder()
            .put("default_validation_class", "UTF8Type")
            .put("key_validation_class",     "UTF8Type")
            .put("comparator_type",          "UTF8Type")
            .build());

            return true;
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        
        return false;
    }
    
    public boolean insertData() {
        
        try {
            
            String rowKey = String.valueOf(System.currentTimeMillis());
            
            MutationBatch m = getContext().prepareMutationBatch();
            
            ColumnFamily<String, String> CF_STANDARD1 = ColumnFamily
                .newColumnFamily("Standard1", StringSerializer.get(),
                        StringSerializer.get());
            
            m.withRow(CF_STANDARD1, rowKey)
                .putColumn("Username", "Jack", null)
                .putColumn("Password", "forgotit", null);
            
            
            OperationResult<Void> result = m.execute();
            
            return true;
            
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        
        return false;
    }
    
    
    public String retreiveData() {
        
        String data = "";
        String cqlCmd = "SELECT * FROM Standard1;";
        
        ColumnFamily<String, String> CF_STANDARD1 = ColumnFamily
                .newColumnFamily("Standard1", StringSerializer.get(),
                        StringSerializer.get());
        try {
            
            OperationResult<CqlResult<String, String>> result
                    = getContext().prepareQuery(CF_STANDARD1)
			.withCql(cqlCmd)
			.execute();
            
            for (Row<String, String> row : result.getResult().getRows()) {
                
                for (Column<String> column : row.getColumns()){

                    data += " ColName: " + column.getName() 
                            + " ColValue: " + column.getValue(new StringSerializer());
                    data += "<br/>";
                }
                
                
                
                return data;
            }
            
        } catch (ConnectionException ex) {
            ex.printStackTrace();
        }
        
	
        
        return "Error";
    }
}
