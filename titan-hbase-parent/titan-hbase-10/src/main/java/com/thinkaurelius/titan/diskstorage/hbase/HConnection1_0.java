package com.thinkaurelius.titan.diskstorage.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;

public class HConnection1_0 implements ConnectionMask
{

    private final Connection cnx;

    public HConnection1_0(Connection cnx)
    {
        this.cnx = cnx;
    }

    @Override
    public TableMask getTable(String name) throws IOException
    {
        return new HTable1_0(cnx.getTable(TableName.valueOf(name)));
    }

    @Override
    public AdminMask getAdmin() throws IOException
    {
        return new HBaseAdmin1_0(cnx.getAdmin());
    }

    @Override
    public void close() throws IOException
    {
        cnx.close();
    }
}
